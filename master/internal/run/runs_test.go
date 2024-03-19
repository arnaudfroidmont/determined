package run

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/determined-ai/determined/master/pkg/model"
)

func TestFlattenMetadata(t *testing.T) {
	data := map[string]interface{}{
		"key1": 1,
		"key2": 2.1,
		"key3": true,
		"key4": "string",
		"key5": "2021-01-01",
		"key6": "2021-01",
		"key7": []interface{}{
			map[string]interface{}{
				"key8": 3,
			},
			[]interface{}{
				"string_1",
				"string_2",
			},
		},
	}
	flattened := FlattenRunMetadata(data)
	require.ElementsMatch(t, []model.RunMetadataIndex{
		{
			RunID:     0,
			FlatKey:   "key1",
			Value:     "1",
			DataType:  "integer",
			ProjectID: 0,
		},
		{
			RunID:     0,
			FlatKey:   "key2",
			Value:     "2.1",
			DataType:  "float",
			ProjectID: 0,
		},
		{
			RunID:     0,
			FlatKey:   "key3",
			Value:     "true",
			DataType:  "boolean",
			ProjectID: 0,
		},
		{
			RunID:     0,
			FlatKey:   "key4",
			Value:     "string",
			DataType:  "string",
			ProjectID: 0,
		},
		{
			RunID:     0,
			FlatKey:   "key5",
			Value:     "2021-01-01T00:00:00Z",
			DataType:  "timestamp",
			ProjectID: 0,
		},
		{
			RunID:     0,
			FlatKey:   "key6",
			Value:     "2021-01-01T00:00:00Z",
			DataType:  "timestamp",
			ProjectID: 0,
		},
		{
			RunID:     0,
			FlatKey:   "key7.key8",
			Value:     "3",
			DataType:  "integer",
			ProjectID: 0,
		},
		{
			RunID:     0,
			FlatKey:   "key7",
			Value:     "string_1",
			DataType:  "string",
			ProjectID: 0,
		},
		{
			RunID:     0,
			FlatKey:   "key7",
			Value:     "string_2",
			DataType:  "string",
			ProjectID: 0,
		},
	}, flattened)
}

func TestFlattenMetadataEmpty(t *testing.T) {
	data := map[string]interface{}{}
	flattened := FlattenRunMetadata(data)
	require.ElementsMatch(t, []model.RunMetadataIndex{}, flattened)
}

func TestFlattenMetadataNil(t *testing.T) {
	flattened := FlattenRunMetadata(nil)
	require.ElementsMatch(t, []model.RunMetadataIndex{}, flattened)
}

func TestFlattenMetadataNested(t *testing.T) {
	data := map[string]interface{}{
		"key1": map[string]interface{}{
			"key2": 1,
		},
	}
	flattened := FlattenRunMetadata(data)
	require.ElementsMatch(t, []model.RunMetadataIndex{
		{
			RunID:     0,
			FlatKey:   "key1.key2",
			Value:     "1",
			DataType:  "integer",
			ProjectID: 0,
		},
	}, flattened)
}

func TestFlattenMetadataArray(t *testing.T) {
	data := map[string]interface{}{
		"key1": []interface{}{
			1,
			2,
		},
	}
	flattened := FlattenRunMetadata(data)
	require.ElementsMatch(t, []model.RunMetadataIndex{
		{
			RunID:     0,
			FlatKey:   "key1",
			Value:     "1",
			DataType:  "integer",
			ProjectID: 0,
		},
		{
			RunID:     0,
			FlatKey:   "key1",
			Value:     "2",
			DataType:  "integer",
			ProjectID: 0,
		},
	}, flattened)
}

func TestFlattenMetadataArrayNested(t *testing.T) {
	data := map[string]interface{}{
		"key1": []interface{}{
			map[string]interface{}{
				"key2": 1,
			},
		},
	}
	flattened := FlattenRunMetadata(data)
	require.ElementsMatch(t, []model.RunMetadataIndex{
		{
			RunID:     0,
			FlatKey:   "key1.key2",
			Value:     "1",
			DataType:  "integer",
			ProjectID: 0,
		},
	}, flattened)
}

func TestMergeRunMetadataOverwriteFailure(t *testing.T) {
	data1 := map[string]interface{}{
		"key1": 1,
		"key2": 2,
	}
	data2 := map[string]interface{}{
		"key2": 3,
		"key3": 4,
	}
	_, err := MergeRunMetadata(data1, data2)
	require.Error(t, err)
}

func TestMergeRunMetadata(t *testing.T) {
	data1 := map[string]interface{}{
		"key1": 1,
		"key2": 2,
	}
	data2 := map[string]interface{}{
		"key3": 3,
		"key4": 4,
	}
	merged, err := MergeRunMetadata(data1, data2)
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{
		"key1": 1,
		"key2": 2,
		"key3": 3,
		"key4": 4,
	}, merged)
}

func TestMergeRunMetadataNested(t *testing.T) {
	data1 := map[string]interface{}{
		"key1": 1,
		"key2": map[string]interface{}{
			"key3": 2,
		},
	}
	data2 := map[string]interface{}{
		"key2": map[string]interface{}{
			"key4": 3,
		},
		"key5": 4,
	}
	merged, err := MergeRunMetadata(data1, data2)
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{
		"key1": 1,
		"key2": map[string]interface{}{
			"key3": 2,
			"key4": 3,
		},
		"key5": 4,
	}, merged)
}

func TestMergeRunMetadataArrayFailure(t *testing.T) {
	data1 := map[string]interface{}{
		"key1": []interface{}{
			1,
			2,
		},
	}
	data2 := map[string]interface{}{
		"key1": []interface{}{
			3,
		},
	}
	_, err := MergeRunMetadata(data1, data2)
	require.ErrorContains(t, err, "unexpected attempt to overwrite existing entry (key1, [1 2]) with new value 3")
}

func TestMergeRunMetadataArray(t *testing.T) {
	data1 := map[string]interface{}{
		"key1": []interface{}{
			1,
			2,
		},
	}
	data2 := map[string]interface{}{
		"key1": map[string]interface{}{
			"key2": 3,
			"key3": 4,
			"key4": 5,
		},
	}
	merged, err := MergeRunMetadata(data1, data2)
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{
		"key1": []interface{}{
			1,
			2,
			map[string]interface{}{
				"key2": 3,
				"key3": 4,
				"key4": 5,
			},
		},
	}, merged)
}

func TestMergeRunMetadataArrayNested(t *testing.T) {
	data1 := map[string]interface{}{
		"key1": map[string]interface{}{
			"key2": 1,
		},
	}
	data2 := map[string]interface{}{
		"key1": map[string]interface{}{
			"key3": 2,
		},
	}
	merged, err := MergeRunMetadata(data1, data2)
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{
		"key1": map[string]interface{}{
			"key2": 1,
			"key3": 2,
		},
	}, merged)
}

func TestMergeRunMetadataArrayNestedList(t *testing.T) {
	data1 := map[string]interface{}{
		"key1": []interface{}{
			map[string]interface{}{
				"key2": 1,
			},
		},
	}
	data2 := map[string]interface{}{
		"key1": []interface{}{
			map[string]interface{}{
				"key3": 2,
			},
		},
	}
	merged, err := MergeRunMetadata(data1, data2)
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{
		"key1": []interface{}{
			map[string]interface{}{
				"key2": 1,
			},
			map[string]interface{}{
				"key3": 2,
			},
		},
	}, merged)
}

func TestMergeRunMetadataArrayNestedListFailure(t *testing.T) {
	data1 := map[string]interface{}{
		"key1": []interface{}{
			map[string]interface{}{
				"key2": 1,
			},
		},
	}
	data2 := map[string]interface{}{
		"key1": []interface{}{
			map[string]interface{}{
				"key2": 2,
			},
		},
	}
	_, err := MergeRunMetadata(data1, data2)
	require.ErrorContains(t, err, "unexpected attempt to overwrite existing entry (key2, 1) with new value 2")
}

func TestMergeRunMetadataArrayNestedListDifferentLength(t *testing.T) {
	data1 := map[string]interface{}{
		"key1": []interface{}{
			map[string]interface{}{
				"key2": 1,
			},
		},
	}
	data2 := map[string]interface{}{
		"key1": []interface{}{
			map[string]interface{}{
				"key3": 2,
			},
			map[string]interface{}{
				"key4": 3,
			},
		},
	}
	merged, err := MergeRunMetadata(data1, data2)
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{
		"key1": []interface{}{
			map[string]interface{}{
				"key2": 1,
			},
			map[string]interface{}{
				"key3": 2,
			},
			map[string]interface{}{
				"key4": 3,
			},
		},
	}, merged)
}

func TestMergeRunMetadataAppendingToPrimitive(t *testing.T) {
	data1 := map[string]interface{}{
		"key1": 1,
	}
	data2 := map[string]interface{}{
		"key1": map[string]interface{}{"key2": 2},
	}
	merged, err := MergeRunMetadata(data1, data2)
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{
		"key1": []interface{}{
			1,
			map[string]interface{}{
				"key2": 2,
			},
		},
	}, merged)
}
