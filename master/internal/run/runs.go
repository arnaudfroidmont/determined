package run

import (
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/determined-ai/determined/master/pkg/model"
)

const (
	integerType   = "integer"
	floatType     = "float"
	booleanType   = "boolean"
	stringType    = "string"
	timestampType = "timestamp"
)

// FlattenRunMetadata flattens a nested map of run metadata into a list of RunMetadataIndex entries.
func FlattenRunMetadata(data map[string]interface{}) []model.RunMetadataIndex {
	return flattenRunMetadata(data, "")
}

// parseMetadataValueType converts a value to a string and returns the type of the value.
func parseMetadataValueType(value interface{}) (string, string) {
	switch v := value.(type) {
	case int:
		return strconv.Itoa(v), integerType
	case float64:
		if v == float64(int(v)) {
			return strconv.Itoa(int(v)), integerType
		}
		return strconv.FormatFloat(v, 'f', -1, 64), floatType
	case bool:
		return strconv.FormatBool(v), booleanType
	case string:
		// TODO (corban): This is a hacky way to determine if a string is a timestamp.
		if _, err := time.Parse(time.RFC3339Nano, v); err == nil {
			return v, "timestamp"
		} else if timestamp, err := time.Parse(time.RFC3339, v); err == nil {
			return timestamp.UTC().Format(time.RFC3339Nano), timestampType
		} else if timestamp, err := time.Parse("2006-01-02", v); err == nil {
			return timestamp.UTC().Format(time.RFC3339Nano), timestampType
		} else if timestamp, err := time.Parse("2006-01", v); err == nil {
			return timestamp.UTC().Format(time.RFC3339Nano), timestampType
		}
		return v, stringType
	default:
		return fmt.Sprintf("%v", v), reflect.TypeOf(value).String()
	}
}

func flattenRunMetadata(data map[string]interface{}, prefix string) []model.RunMetadataIndex {
	var flattened []model.RunMetadataIndex

	for key, value := range data {
		newKey := fmt.Sprintf("%s%s", prefix, key)
		switch valueType := value.(type) {
		// If the value is a map, recursively flatten it.
		case map[string]interface{}:
			flattened = append(flattened, flattenRunMetadata(value.(map[string]interface{}), newKey+".")...)
		// If the value is a slice, iterate over it and recursively flatten each element.
		case []interface{}:
			for _, v := range valueType {
				switch vType := v.(type) {
				case map[string]interface{}:
					flattened = append(flattened, flattenRunMetadata(vType, newKey+".")...)
				case []interface{}:
					flattened = append(flattened, flattenRunMetadata(map[string]interface{}{newKey: vType}, "")...)
				default:
					val, valType := parseMetadataValueType(v)
					flattened = append(flattened, model.RunMetadataIndex{FlatKey: newKey, Value: val, DataType: valType})
				}
			}
		// If the value is a primitive, add it to the flattened list.
		default:
			val, valType := parseMetadataValueType(value)
			flattened = append(flattened, model.RunMetadataIndex{FlatKey: newKey, Value: val, DataType: valType})
		}
	}
	return flattened
}
