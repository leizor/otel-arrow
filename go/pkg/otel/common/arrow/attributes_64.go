package arrow

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/config"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/builder"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/constants"
	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

var (
	AttrsSchema64 = arrow.NewSchema([]arrow.Field{
		{Name: constants.ParentID, Type: arrow.PrimitiveTypes.Uint64, Metadata: schema.Metadata(schema.Dictionary8)},
		{Name: constants.AttributeKey, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Dictionary8)},
		{Name: constants.AttributeType, Type: arrow.PrimitiveTypes.Uint8},
		{Name: constants.AttributeStr, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Dictionary16), Nullable: true},
		{Name: constants.AttributeInt, Type: arrow.PrimitiveTypes.Int64, Metadata: schema.Metadata(schema.Dictionary16), Nullable: true},
		{Name: constants.AttributeDouble, Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: constants.AttributeBool, Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: constants.AttributeBytes, Type: arrow.BinaryTypes.Binary, Metadata: schema.Metadata(schema.Dictionary16), Nullable: true},
		{Name: constants.AttributeSer, Type: arrow.BinaryTypes.Binary, Metadata: schema.Metadata(schema.Dictionary16), Nullable: true},
	}, nil)

	DeltaEncodedAttrsSchema64 = arrow.NewSchema([]arrow.Field{
		{Name: constants.ParentID, Type: arrow.PrimitiveTypes.Uint64, Metadata: schema.Metadata(schema.Dictionary8, schema.DeltaEncoding)},
		{Name: constants.AttributeKey, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Dictionary8)},
		{Name: constants.AttributeType, Type: arrow.PrimitiveTypes.Uint8},
		{Name: constants.AttributeStr, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Dictionary16), Nullable: true},
		{Name: constants.AttributeInt, Type: arrow.PrimitiveTypes.Int64, Metadata: schema.Metadata(schema.Dictionary16), Nullable: true},
		{Name: constants.AttributeDouble, Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: constants.AttributeBool, Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: constants.AttributeBytes, Type: arrow.BinaryTypes.Binary, Metadata: schema.Metadata(schema.Dictionary16), Nullable: true},
		{Name: constants.AttributeSer, Type: arrow.BinaryTypes.Binary, Metadata: schema.Metadata(schema.Dictionary16), Nullable: true},
	}, nil)
)

type Attrs64Builder struct {
	released bool

	builder *builder.RecordBuilderExt // Record builder

	pib   *builder.Uint64Builder
	keyb  *builder.StringBuilder
	typeb *builder.Uint8Builder
	strb  *builder.StringBuilder
	i64b  *builder.Int64Builder
	f64b  *builder.Float64Builder
	boolb *builder.BooleanBuilder
	binb  *builder.BinaryBuilder
	serb  *builder.BinaryBuilder

	accumulator *Attributes64Accumulator
	payloadType *PayloadType
}

type Attrs64ByNothing struct{}

type Attrs64ByTypeParentIdKeyValue struct {
	prevParentID uint64
	prevValue    *pcommon.Value
}

type Attrs64ByTypeKeyParentIdValue struct {
	prevParentID uint64
	prevKey      string
	prevValue    *pcommon.Value
}

type Attrs64ByTypeKeyValueParentId struct {
	prevParentID uint64
	prevKey      string
	prevValue    *pcommon.Value
}

type Attrs64ByKeyValueParentId struct {
	prevParentID uint64
	prevKey      string
	prevValue    *pcommon.Value
}

// Attrs64FindOrderByFunc returns the sorter for the given order by
func Attrs64FindOrderByFunc(orderBy config.OrderAttrs64By) Attrs64Sorter {
	switch orderBy {
	case config.OrderAttrs64ByNothing:
		return &Attrs64ByNothing{}
	case config.OrderAttrs64ByTypeKeyParentIdValue:
		return &Attrs64ByTypeKeyParentIdValue{}
	case config.OrderAttrs64ByTypeKeyValueParentId:
		return &Attrs64ByTypeKeyValueParentId{}
	default:
		panic(fmt.Sprintf("unknown OrderAttrs64By variant: %d", orderBy))
	}
}

// No sorting
// ==========

func UnsortedAttrs64() *Attrs64ByNothing {
	return &Attrs64ByNothing{}
}

func (s *Attrs64ByNothing) Sort(_ []Attr64) []string {
	// Do nothing
	return []string{}
}

func (s *Attrs64ByNothing) Encode(parentID uint64, _ string, _ *pcommon.Value) uint64 {
	return parentID
}

func (s *Attrs64ByNothing) Reset() {}

// Sorts the attributes by type, key, parentID, and value
// ================================================

func SortAttrs64ByTypeKeyParentIdValue() *Attrs64ByTypeKeyParentIdValue {
	return &Attrs64ByTypeKeyParentIdValue{}
}

func (s *Attrs64ByTypeKeyParentIdValue) Sort(attrs []Attr64) []string {
	sort.Slice(attrs, func(i, j int) bool {
		attrsI := attrs[i]
		attrsJ := attrs[j]
		if attrsI.Value.Type() == attrsJ.Value.Type() {
			if attrsI.Key == attrsJ.Key {
				if attrsI.ParentID == attrsJ.ParentID {
					return IsLess(attrsI.Value, attrsJ.Value)
				} else {
					return attrsI.ParentID < attrsJ.ParentID
				}
			} else {
				return attrsI.Key < attrsJ.Key
			}
		} else {
			return attrsI.Value.Type() < attrsJ.Value.Type()
		}
	})

	return []string{constants.AttributeType, constants.AttributeKey, constants.ParentID, constants.Value}
}

func (s *Attrs64ByTypeKeyParentIdValue) Encode(parentID uint64, key string, value *pcommon.Value) uint64 {
	if s.prevValue == nil {
		s.prevKey = key
		s.prevValue = value
		s.prevParentID = parentID
		return parentID
	}

	if s.IsSameGroup(key, value) {
		delta := parentID - s.prevParentID
		s.prevParentID = parentID
		return delta
	} else {
		s.prevKey = key
		s.prevValue = value
		s.prevParentID = parentID
		return parentID
	}
}

func (s *Attrs64ByTypeKeyParentIdValue) Reset() {
	s.prevParentID = 0
	s.prevKey = ""
	s.prevValue = nil
}

func (s *Attrs64ByTypeKeyParentIdValue) IsSameGroup(key string, value *pcommon.Value) bool {
	if s.prevValue == nil {
		return false
	}

	if s.prevValue.Type() == value.Type() && s.prevKey == key {
		return true
	}
	return false
}

// Sorts the attributes by type, key, value, and parentID
// ================================================

func SortAttrs64ByTypeKeyValueParentId() *Attrs64ByTypeKeyValueParentId {
	return &Attrs64ByTypeKeyValueParentId{}
}

func (s *Attrs64ByTypeKeyValueParentId) Sort(attrs []Attr64) []string {
	sort.Slice(attrs, func(i, j int) bool {
		attrsI := attrs[i]
		attrsJ := attrs[j]
		if attrsI.Value.Type() == attrsJ.Value.Type() {
			if attrsI.Key == attrsJ.Key {
				cmp := Compare(attrsI.Value, attrsJ.Value)
				if cmp == 0 {
					return attrsI.ParentID < attrsJ.ParentID
				} else {
					return cmp < 0
				}
			} else {
				return attrsI.Key < attrsJ.Key
			}
		} else {
			return attrsI.Value.Type() < attrsJ.Value.Type()
		}
	})
	return []string{constants.AttributeType, constants.AttributeKey, constants.Value, constants.ParentID}
}

func (s *Attrs64ByTypeKeyValueParentId) Encode(parentID uint64, key string, value *pcommon.Value) uint64 {
	if s.prevValue == nil {
		s.prevKey = key
		s.prevValue = value
		s.prevParentID = parentID
		return parentID
	}

	if s.IsSameGroup(key, value) {
		delta := parentID - s.prevParentID
		s.prevParentID = parentID
		return delta
	} else {
		s.prevKey = key
		s.prevValue = value
		s.prevParentID = parentID
		return parentID
	}
}

func (s *Attrs64ByTypeKeyValueParentId) Reset() {
	s.prevParentID = 0
	s.prevKey = ""
	s.prevValue = nil
}

func (s *Attrs64ByTypeKeyValueParentId) IsSameGroup(key string, value *pcommon.Value) bool {
	if s.prevValue == nil {
		return false
	}

	if s.prevKey == key {
		return Equal(s.prevValue, value)
	} else {
		return false
	}
}

func NewAttrs64Builder(rBuilder *builder.RecordBuilderExt, payloadType *PayloadType, sorter Attrs64Sorter) *Attrs64Builder {
	b := &Attrs64Builder{
		builder:     rBuilder,
		accumulator: NewAttributes64Accumulator(sorter),
		payloadType: payloadType,
	}
	b.init()

	return b
}

func (b *Attrs64Builder) init() {
	b.pib = b.builder.Uint64Builder(constants.ParentID)
	b.keyb = b.builder.StringBuilder(constants.AttributeKey)
	b.typeb = b.builder.Uint8Builder(constants.AttributeType)
	b.strb = b.builder.StringBuilder(constants.AttributeStr)
	b.i64b = b.builder.Int64Builder(constants.AttributeInt)
	b.f64b = b.builder.Float64Builder(constants.AttributeDouble)
	b.boolb = b.builder.BooleanBuilder(constants.AttributeBool)
	b.binb = b.builder.BinaryBuilder(constants.AttributeBytes)
	b.serb = b.builder.BinaryBuilder(constants.AttributeSer)
}

func (b *Attrs64Builder) Accumulator() *Attributes64Accumulator {
	return b.accumulator
}

func (b *Attrs64Builder) IsEmpty() bool {
	return b.accumulator.IsEmpty()
}

func (b *Attrs64Builder) TryBuild() (record arrow.Record, err error) {
	if b.released {
		return nil, werror.Wrap(ErrBuilderAlreadyReleased)
	}

	sortingColumns := b.accumulator.Sort()
	b.builder.AddMetadata(constants.SortingColumns, strings.Join(sortingColumns, ","))

	b.builder.Reserve(len(b.accumulator.attrs))

	for _, attr := range b.accumulator.attrs {
		b.pib.Append(b.accumulator.sorter.Encode(attr.ParentID, attr.Key, attr.Value))
		b.keyb.Append(attr.Key)

		switch attr.Value.Type() {
		case pcommon.ValueTypeStr:
			b.typeb.Append(uint8(pcommon.ValueTypeStr))
			b.strb.Append(attr.Value.Str())
			b.i64b.AppendNull()
			b.f64b.AppendNull()
			b.boolb.AppendNull()
			b.binb.AppendNull()
			b.serb.AppendNull()
		case pcommon.ValueTypeInt:
			b.typeb.Append(uint8(pcommon.ValueTypeInt))
			b.i64b.Append(attr.Value.Int())
			b.strb.AppendNull()
			b.f64b.AppendNull()
			b.boolb.AppendNull()
			b.binb.AppendNull()
			b.serb.AppendNull()
		case pcommon.ValueTypeDouble:
			b.typeb.Append(uint8(pcommon.ValueTypeDouble))
			b.f64b.Append(attr.Value.Double())
			b.strb.AppendNull()
			b.i64b.AppendNull()
			b.boolb.AppendNull()
			b.binb.AppendNull()
			b.serb.AppendNull()
		case pcommon.ValueTypeBool:
			b.typeb.Append(uint8(pcommon.ValueTypeBool))
			b.boolb.Append(attr.Value.Bool())
			b.strb.AppendNull()
			b.i64b.AppendNull()
			b.f64b.AppendNull()
			b.binb.AppendNull()
			b.serb.AppendNull()
		case pcommon.ValueTypeBytes:
			b.typeb.Append(uint8(pcommon.ValueTypeBytes))
			b.binb.Append(attr.Value.Bytes().AsRaw())
			b.strb.AppendNull()
			b.i64b.AppendNull()
			b.f64b.AppendNull()
			b.boolb.AppendNull()
			b.serb.AppendNull()
		case pcommon.ValueTypeSlice:
			cborData, err := common.Serialize(attr.Value)
			if err != nil {
				break
			}
			b.typeb.Append(uint8(pcommon.ValueTypeSlice))
			b.serb.Append(cborData)
			b.strb.AppendNull()
			b.i64b.AppendNull()
			b.f64b.AppendNull()
			b.boolb.AppendNull()
			b.binb.AppendNull()
		case pcommon.ValueTypeMap:
			cborData, err := common.Serialize(attr.Value)
			if err != nil {
				break
			}
			b.typeb.Append(uint8(pcommon.ValueTypeMap))
			b.serb.Append(cborData)
			b.strb.AppendNull()
			b.i64b.AppendNull()
			b.f64b.AppendNull()
			b.boolb.AppendNull()
			b.binb.AppendNull()
		}
	}

	record, err = b.builder.NewRecord()
	if err != nil {
		b.init()
	}

	return
}

func (b *Attrs64Builder) Build() (arrow.Record, error) {
	schemaNotUpToDateCount := 0

	var record arrow.Record
	var err error

	// Loop until the record is built successfully.
	// Intermediaries steps may be required to update the schema.
	for {
		record, err = b.TryBuild()
		if err != nil {
			if record != nil {
				record.Release()
			}

			switch {
			case errors.Is(err, schema.ErrSchemaNotUpToDate):
				schemaNotUpToDateCount++
				if schemaNotUpToDateCount > 5 {
					panic("Too many consecutive schema updates. This shouldn't happen.")
				}
			default:
				return nil, werror.Wrap(err)
			}
		} else {
			break
		}
	}

	return record, werror.Wrap(err)
}

func (b *Attrs64Builder) SchemaID() string {
	return b.builder.SchemaID()
}

func (b *Attrs64Builder) Schema() *arrow.Schema {
	return b.builder.Schema()
}

func (b *Attrs64Builder) PayloadType() *PayloadType {
	return b.payloadType
}

func (b *Attrs64Builder) Reset() {
	b.accumulator.Reset()
}

// Release releases the memory allocated by the builder.
func (b *Attrs64Builder) Release() {
	if !b.released {
		b.builder.Release()
		b.released = true
	}
}

func (b *Attrs64Builder) ShowSchema() {
	b.builder.ShowSchema()
}

type Attr64 struct {
	ParentID uint64
	Key      string
	Value    *pcommon.Value
}

type Attrs64Sorter interface {
	Sort(attrs []Attr64) []string
	Encode(parentID uint64, key string, value *pcommon.Value) uint64
	Reset()
}

type Attributes64Accumulator struct {
	attrsMapCount uint64
	attrs         []Attr64
	sorter        Attrs64Sorter
}

func NewAttributes64Accumulator(sorter Attrs64Sorter) *Attributes64Accumulator {
	return &Attributes64Accumulator{
		attrs:  make([]Attr64, 0),
		sorter: sorter,
	}
}

func (c *Attributes64Accumulator) IsEmpty() bool {
	return len(c.attrs) == 0
}

func (c *Attributes64Accumulator) AppendWithID(parentID uint64, attrs pcommon.Map) error {
	if attrs.Len() == 0 {
		return nil
	}

	attrs.Range(func(key string, v pcommon.Value) bool {
		if key == "" || v.Type() == pcommon.ValueTypeEmpty {
			// Attribute without key or without value is ignored.
			return true
		}

		c.attrs = append(c.attrs, Attr64{
			ParentID: parentID,
			Key:      key,
			Value:    &v,
		})

		return true
	})

	c.attrsMapCount++

	if c.attrsMapCount == math.MaxUint64 {
		panic("The maximum number of group attributes has been reached (max is uint64).")
	}

	return nil
}

func (c *Attributes64Accumulator) Sort() []string {
	sortingColumns := c.sorter.Sort(c.attrs)
	c.sorter.Reset()
	return sortingColumns
}

func (c *Attributes64Accumulator) Reset() {
	c.attrsMapCount = 0
	c.attrs = c.attrs[:0]
}
