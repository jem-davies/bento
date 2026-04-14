package confluent

import (
	"encoding/json"
	"testing"

	"github.com/nsf/jsondiff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateNamespaces(t *testing.T) {
	tests := map[string]struct {
		testSchema    string
		cleanedSchema string
	}{
		// test we are not altering "references"
		"schemaWithDodgyNamespaceNamesWithReferences": {
			testSchema: `{
	"schema": {
		"type": "record",
		"name": "User",
		"namespace": "com.example-dodgy",
		"fields": [
			{
				"name": "name",
				"type": "string"
			},
			{
				"name": "email",
				"type": "string"
			},
			{
				"name": "address",
				"type": "Address-dodgy"
			}
		]
	},
	"references": {
		"namespace": "com.example-dodgy",
		"name": "Address-dodgy",
		"subject": "Address-dodgy",
		"version": 1
	}
}`,
			cleanedSchema: `{
	"schema": {
		"type": "record",
		"name": "User",
		"namespace": "com.exampledodgy",
		"fields": [
			{
				"name": "name",
				"type": "string"
			},
			{
				"name": "email",
				"type": "string"
			},
			{
				"name": "address",
				"type": "Address-dodgy"
			}
		]
	},
	"references": {
		"namespace": "com.example-dodgy",
		"name": "Address-dodgy",
		"subject": "Address-dodgy",
		"version": 1
	}
}`,
		},
		// Test we are only fixing "namespaces" not names
		"dodgyNameSpaceAndDodgyName": {
			testSchema: `{
	"schema": {
		"type": "record",
		"name": "User",
		"namespace": "com.example",
		"fields": [
			{
				"name": "com.example-dodgy.dodgy-name",
				"type": "string"
			},
			{	
				"namespace": "com.example-dodgy",
				"name": "age",
				"type": "int"
			}
		]
	}
}`,
			cleanedSchema: `{
	"schema": {
		"type": "record",
		"name": "User",
		"namespace": "com.example",
		"fields": [
			{
				"name": "com.exampledodgy.dodgy-name",
				"type": "string"
			},
			{	
				"namespace": "com.exampledodgy",
				"name": "age",
				"type": "int"
			}
		]
	}
}`,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			type container struct {
				Schema json.RawMessage `json:"schema"`
			}
			var envelope container
			require.NoError(t, json.Unmarshal([]byte(test.testSchema), &envelope))

			var cleanedEnvelope container
			require.NoError(t, json.Unmarshal([]byte(test.cleanedSchema), &cleanedEnvelope))

			schema := SchemaInfo{Schema: string(envelope.Schema)}
			require.NoError(t, schema.SanitizeSchema())

			jdopts := jsondiff.DefaultJSONOptions()
			diff, explanation := jsondiff.Compare([]byte(schema.Schema), cleanedEnvelope.Schema, &jdopts)
			assert.Equalf(t, jsondiff.FullMatch.String(), diff.String(), "%s: %s", name, explanation)
		})
	}
}
