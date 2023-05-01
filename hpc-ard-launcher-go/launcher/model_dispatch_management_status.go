/*
Launcher API

The Launcher API is the execution layer for the Capsules framework.  It handles all the details of launching and monitoring runtime environments.

API version: 3.2.8
*/

// Code generated by OpenAPI Generator (https://openapi-generator.tech); DO NOT EDIT.

package launcher

import (
	"encoding/json"
)

// DispatchManagementStatus struct for DispatchManagementStatus
type DispatchManagementStatus struct {
	Info *DispatchInfo `json:"info,omitempty"`
	Dispatcher *string `json:"dispatcher,omitempty"`
	Reason *string `json:"reason,omitempty"`
	CanManage *bool `json:"canManage,omitempty"`
	AdditionalPropertiesField *map[string]interface{} `json:"additionalProperties,omitempty"`
}

// NewDispatchManagementStatus instantiates a new DispatchManagementStatus object
// This constructor will assign default values to properties that have it defined,
// and makes sure properties required by API are set, but the set of arguments
// will change when the set of required properties is changed
func NewDispatchManagementStatus() *DispatchManagementStatus {
	this := DispatchManagementStatus{}
	return &this
}

// NewDispatchManagementStatusWithDefaults instantiates a new DispatchManagementStatus object
// This constructor will only assign default values to properties that have it defined,
// but it doesn't guarantee that properties required by API are set
func NewDispatchManagementStatusWithDefaults() *DispatchManagementStatus {
	this := DispatchManagementStatus{}
	return &this
}

// GetInfo returns the Info field value if set, zero value otherwise.
func (o *DispatchManagementStatus) GetInfo() DispatchInfo {
	if o == nil || o.Info == nil {
		var ret DispatchInfo
		return ret
	}
	return *o.Info
}

// GetInfoOk returns a tuple with the Info field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *DispatchManagementStatus) GetInfoOk() (*DispatchInfo, bool) {
	if o == nil || o.Info == nil {
		return nil, false
	}
	return o.Info, true
}

// HasInfo returns a boolean if a field has been set.
func (o *DispatchManagementStatus) HasInfo() bool {
	if o != nil && o.Info != nil {
		return true
	}

	return false
}

// SetInfo gets a reference to the given DispatchInfo and assigns it to the Info field.
func (o *DispatchManagementStatus) SetInfo(v DispatchInfo) {
	o.Info = &v
}

// GetDispatcher returns the Dispatcher field value if set, zero value otherwise.
func (o *DispatchManagementStatus) GetDispatcher() string {
	if o == nil || o.Dispatcher == nil {
		var ret string
		return ret
	}
	return *o.Dispatcher
}

// GetDispatcherOk returns a tuple with the Dispatcher field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *DispatchManagementStatus) GetDispatcherOk() (*string, bool) {
	if o == nil || o.Dispatcher == nil {
		return nil, false
	}
	return o.Dispatcher, true
}

// HasDispatcher returns a boolean if a field has been set.
func (o *DispatchManagementStatus) HasDispatcher() bool {
	if o != nil && o.Dispatcher != nil {
		return true
	}

	return false
}

// SetDispatcher gets a reference to the given string and assigns it to the Dispatcher field.
func (o *DispatchManagementStatus) SetDispatcher(v string) {
	o.Dispatcher = &v
}

// GetReason returns the Reason field value if set, zero value otherwise.
func (o *DispatchManagementStatus) GetReason() string {
	if o == nil || o.Reason == nil {
		var ret string
		return ret
	}
	return *o.Reason
}

// GetReasonOk returns a tuple with the Reason field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *DispatchManagementStatus) GetReasonOk() (*string, bool) {
	if o == nil || o.Reason == nil {
		return nil, false
	}
	return o.Reason, true
}

// HasReason returns a boolean if a field has been set.
func (o *DispatchManagementStatus) HasReason() bool {
	if o != nil && o.Reason != nil {
		return true
	}

	return false
}

// SetReason gets a reference to the given string and assigns it to the Reason field.
func (o *DispatchManagementStatus) SetReason(v string) {
	o.Reason = &v
}

// GetCanManage returns the CanManage field value if set, zero value otherwise.
func (o *DispatchManagementStatus) GetCanManage() bool {
	if o == nil || o.CanManage == nil {
		var ret bool
		return ret
	}
	return *o.CanManage
}

// GetCanManageOk returns a tuple with the CanManage field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *DispatchManagementStatus) GetCanManageOk() (*bool, bool) {
	if o == nil || o.CanManage == nil {
		return nil, false
	}
	return o.CanManage, true
}

// HasCanManage returns a boolean if a field has been set.
func (o *DispatchManagementStatus) HasCanManage() bool {
	if o != nil && o.CanManage != nil {
		return true
	}

	return false
}

// SetCanManage gets a reference to the given bool and assigns it to the CanManage field.
func (o *DispatchManagementStatus) SetCanManage(v bool) {
	o.CanManage = &v
}

// GetAdditionalPropertiesField returns the AdditionalPropertiesField field value if set, zero value otherwise.
func (o *DispatchManagementStatus) GetAdditionalPropertiesField() map[string]interface{} {
	if o == nil || o.AdditionalPropertiesField == nil {
		var ret map[string]interface{}
		return ret
	}
	return *o.AdditionalPropertiesField
}

// GetAdditionalPropertiesFieldOk returns a tuple with the AdditionalPropertiesField field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *DispatchManagementStatus) GetAdditionalPropertiesFieldOk() (*map[string]interface{}, bool) {
	if o == nil || o.AdditionalPropertiesField == nil {
		return nil, false
	}
	return o.AdditionalPropertiesField, true
}

// HasAdditionalPropertiesField returns a boolean if a field has been set.
func (o *DispatchManagementStatus) HasAdditionalPropertiesField() bool {
	if o != nil && o.AdditionalPropertiesField != nil {
		return true
	}

	return false
}

// SetAdditionalPropertiesField gets a reference to the given map[string]interface{} and assigns it to the AdditionalPropertiesField field.
func (o *DispatchManagementStatus) SetAdditionalPropertiesField(v map[string]interface{}) {
	o.AdditionalPropertiesField = &v
}

func (o DispatchManagementStatus) MarshalJSON() ([]byte, error) {
	toSerialize := map[string]interface{}{}
	if o.Info != nil {
		toSerialize["info"] = o.Info
	}
	if o.Dispatcher != nil {
		toSerialize["dispatcher"] = o.Dispatcher
	}
	if o.Reason != nil {
		toSerialize["reason"] = o.Reason
	}
	if o.CanManage != nil {
		toSerialize["canManage"] = o.CanManage
	}
	if o.AdditionalPropertiesField != nil {
		toSerialize["additionalProperties"] = o.AdditionalPropertiesField
	}
	return json.Marshal(toSerialize)
}

type NullableDispatchManagementStatus struct {
	value *DispatchManagementStatus
	isSet bool
}

func (v NullableDispatchManagementStatus) Get() *DispatchManagementStatus {
	return v.value
}

func (v *NullableDispatchManagementStatus) Set(val *DispatchManagementStatus) {
	v.value = val
	v.isSet = true
}

func (v NullableDispatchManagementStatus) IsSet() bool {
	return v.isSet
}

func (v *NullableDispatchManagementStatus) Unset() {
	v.value = nil
	v.isSet = false
}

func NewNullableDispatchManagementStatus(val *DispatchManagementStatus) *NullableDispatchManagementStatus {
	return &NullableDispatchManagementStatus{value: val, isSet: true}
}

func (v NullableDispatchManagementStatus) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.value)
}

func (v *NullableDispatchManagementStatus) UnmarshalJSON(src []byte) error {
	v.isSet = true
	return json.Unmarshal(src, &v.value)
}


