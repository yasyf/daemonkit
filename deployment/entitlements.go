package deployment

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"

	"howett.net/plist"
)

// DigestEntitlements returns the canonical digest of one complete entitlement dictionary.
func DigestEntitlements(payload []byte) (SHA256, error) {
	var value map[string]any
	if _, err := plist.Unmarshal(payload, &value); err != nil {
		return SHA256{}, fmt.Errorf("deployment: parse entitlements: %w", err)
	}
	if value == nil {
		return SHA256{}, errors.New("deployment: entitlements must be a dictionary")
	}
	canonical, err := canonicalEntitlementValue(value)
	if err != nil {
		return SHA256{}, err
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return SHA256{}, fmt.Errorf("deployment: encode entitlements: %w", err)
	}
	return sha256.Sum256(encoded), nil
}

func canonicalEntitlementValue(value any) (any, error) {
	switch typed := value.(type) {
	case nil, bool, string:
		return typed, nil
	case []byte:
		return map[string]any{"data": typed}, nil
	case time.Time:
		return map[string]any{"date": typed.UTC().Format(time.RFC3339Nano)}, nil
	case []any:
		result := make([]any, len(typed))
		for index, item := range typed {
			canonical, err := canonicalEntitlementValue(item)
			if err != nil {
				return nil, err
			}
			result[index] = canonical
		}
		return result, nil
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, item := range typed {
			canonical, err := canonicalEntitlementValue(item)
			if err != nil {
				return nil, err
			}
			result[key] = canonical
		}
		return result, nil
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return map[string]any{"integer": reflected.Int()}, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if reflected.Uint() > math.MaxInt64 {
			return map[string]any{"unsigned": fmt.Sprintf("%d", reflected.Uint())}, nil
		}
		return map[string]any{"integer": json.Number(strconv.FormatUint(reflected.Uint(), 10))}, nil
	case reflect.Float32, reflect.Float64:
		if math.IsInf(reflected.Float(), 0) || math.IsNaN(reflected.Float()) {
			return nil, errors.New("deployment: entitlement contains non-finite real")
		}
		return map[string]any{"real": reflected.Float()}, nil
	case reflect.Map:
		if reflected.Type().Key().Kind() != reflect.String {
			break
		}
		result := make(map[string]any, reflected.Len())
		iterator := reflected.MapRange()
		for iterator.Next() {
			canonical, err := canonicalEntitlementValue(iterator.Value().Interface())
			if err != nil {
				return nil, err
			}
			result[iterator.Key().String()] = canonical
		}
		return result, nil
	case reflect.Slice, reflect.Array:
		result := make([]any, reflected.Len())
		for index := range reflected.Len() {
			canonical, err := canonicalEntitlementValue(reflected.Index(index).Interface())
			if err != nil {
				return nil, err
			}
			result[index] = canonical
		}
		return result, nil
	}
	return nil, fmt.Errorf("deployment: unsupported entitlement value %T", value)
}

func decodeEntitlementsOutput(output []byte) (SHA256, error) {
	start := bytes.Index(output, []byte("<?xml"))
	if start < 0 {
		start = bytes.Index(output, []byte("bplist"))
	}
	if start < 0 {
		return SHA256{}, errors.New("deployment: codesign returned no entitlement plist")
	}
	return DigestEntitlements(output[start:])
}
