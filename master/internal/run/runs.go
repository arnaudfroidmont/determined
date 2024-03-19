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

// MergeRunMetadata two sets of run metadata together.
func MergeRunMetadata(current, addition map[string]interface{}) (map[string]interface{}, error) {
	for newKey, newVal := range addition {
		// we have a matched key
		if oldVal, ok := current[newKey]; ok {
			switch typedNewVal := newVal.(type) {
			// the new value is nested, so we'll eventually recursively join.
			case map[string]interface{}:
				switch typedOldVal := oldVal.(type) {
				case map[string]interface{}:
					temp, err := MergeRunMetadata(typedOldVal, typedNewVal)
					if err != nil {
						return nil, err
					}
					current[newKey] = temp
				case []interface{}:
					// if the old value is a list, but the new value is a map,
					// then we want to search the old list for maps that intersect with the current map.
					found := make(map[string]struct{})
					notFound := make(map[string]interface{})
					for nestedNewKey, nestedNewVal := range typedNewVal {
						for i, knownElem := range typedOldVal {
							switch typedKnownElem := knownElem.(type) {
							// we found a map inside the old list, check if there's an intersection
							case map[string]interface{}:
								if _, ok := typedKnownElem[nestedNewKey]; ok {
									found[nestedNewKey] = struct{}{}
									temp, err := MergeRunMetadata(typedKnownElem, map[string]interface{}{nestedNewKey: nestedNewVal})
									if err != nil {
										return nil, err
									}
									typedOldVal[i] = temp
								}
							}
						}
						if _, ok := found[nestedNewKey]; !ok {
							// a map with the same key as the new map was not found in the old list,
							// so we'll eventually append it to the list.
							notFound[nestedNewKey] = nestedNewVal
						}
					}
					typedOldVal = append(typedOldVal, notFound)
					current[newKey] = typedOldVal
				default:
					current[newKey] = append([]interface{}{oldVal}, newVal)
				}
			case []interface{}:
				// treat each element as if it was a kv pair and recursively merge the new value into the old value.
				for _, newElem := range typedNewVal {
					merged, err := MergeRunMetadata(map[string]interface{}{newKey: oldVal}, map[string]interface{}{newKey: newElem})
					if err != nil {
						return nil, err
					}
					oldVal = merged[newKey]
				}
				current[newKey] = oldVal
			default:
				return nil, fmt.Errorf(
					"unexpected attempt to overwrite existing entry (%s, %v) with new value %v",
					newKey,
					oldVal,
					newVal,
				)
			}
		} else {
			// Add in new key-value pairs that are not in the old metadata.
			current[newKey] = newVal
		}
	}
	return current, nil
}
