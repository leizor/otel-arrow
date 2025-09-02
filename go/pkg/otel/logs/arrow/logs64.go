package arrow

import (
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common"
	acommon "github.com/open-telemetry/otel-arrow/go/pkg/otel/common/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/builder"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/constants"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/observer"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/stats"
	"github.com/open-telemetry/otel-arrow/go/pkg/record_message"
	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

var (
	// Logs64Schema is an Arrow schema for OTLP Arrow Logs records that use Uint64 as ID fields.
	Logs64Schema = arrow.NewSchema([]arrow.Field{
		{Name: constants.ID, Type: arrow.PrimitiveTypes.Uint64, Metadata: schema.Metadata(schema.Optional, schema.DeltaEncoding), Nullable: true},
		{Name: constants.Resource, Type: acommon.Resource64DT, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.Scope, Type: acommon.Scope64DT, Metadata: schema.Metadata(schema.Optional)},
		// This schema URL applies to the span and span events (the schema URL
		// for the resource is in the resource struct).
		{Name: constants.SchemaUrl, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Optional, schema.Dictionary8)},
		{Name: constants.TimeUnixNano, Type: arrow.FixedWidthTypes.Timestamp_ns},
		{Name: constants.ObservedTimeUnixNano, Type: arrow.FixedWidthTypes.Timestamp_ns},
		{Name: constants.TraceId, Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}, Metadata: schema.Metadata(schema.Optional, schema.Dictionary8)},
		{Name: constants.SpanId, Type: &arrow.FixedSizeBinaryType{ByteWidth: 8}, Metadata: schema.Metadata(schema.Optional, schema.Dictionary8)},
		{Name: constants.SeverityNumber, Type: arrow.PrimitiveTypes.Int32, Metadata: schema.Metadata(schema.Optional, schema.Dictionary8), Nullable: true},
		{Name: constants.SeverityText, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Optional, schema.Dictionary8), Nullable: true},
		{Name: constants.Body, Type: arrow.StructOf([]arrow.Field{
			{Name: constants.BodyType, Type: arrow.PrimitiveTypes.Uint8, Nullable: true},
			{Name: constants.BodyStr, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Dictionary16), Nullable: true},
			{Name: constants.BodyInt, Type: arrow.PrimitiveTypes.Int64, Metadata: schema.Metadata(schema.Optional, schema.Dictionary16), Nullable: true},
			{Name: constants.BodyDouble, Type: arrow.PrimitiveTypes.Float64, Metadata: schema.Metadata(schema.Optional), Nullable: true},
			{Name: constants.BodyBool, Type: arrow.FixedWidthTypes.Boolean, Metadata: schema.Metadata(schema.Optional), Nullable: true},
			{Name: constants.BodyBytes, Type: arrow.BinaryTypes.Binary, Metadata: schema.Metadata(schema.Optional, schema.Dictionary16), Nullable: true},
			{Name: constants.BodySer, Type: arrow.BinaryTypes.Binary, Metadata: schema.Metadata(schema.Optional, schema.Dictionary16), Nullable: true},
		}...), Nullable: true},
		{Name: constants.DroppedAttributesCount, Type: arrow.PrimitiveTypes.Uint32, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.Flags, Type: arrow.PrimitiveTypes.Uint32, Metadata: schema.Metadata(schema.Optional)},
	}, nil)
)

type RelatedData64 struct {
	relatedRecordsManager *acommon.RelatedRecordsManager
	attrsBuilders         *Attrs64Builders
}

func NewRelatedData64(cfg *Config, stats *stats.ProducerStats, observer observer.ProducerObserver) *RelatedData64 {
	rr := acommon.NewRelatedRecordsManager(cfg.Global, stats)

	resource := rr.Declare(acommon.PayloadTypes.ResourceAttrs, acommon.PayloadTypes.Logs, acommon.AttrsSchema64, func(b *builder.RecordBuilderExt) acommon.RelatedRecordBuilder {
		return acommon.NewAttrs64Builder(b, acommon.PayloadTypes.ResourceAttrs, cfg.Attrs64.Resource.Sorter)
	}, observer)

	scope := rr.Declare(acommon.PayloadTypes.ScopeAttrs, acommon.PayloadTypes.Logs, acommon.AttrsSchema64, func(b *builder.RecordBuilderExt) acommon.RelatedRecordBuilder {
		return acommon.NewAttrs64Builder(b, acommon.PayloadTypes.ScopeAttrs, cfg.Attrs64.Scope.Sorter)
	}, observer)

	logRecord := rr.Declare(acommon.PayloadTypes.LogRecordAttrs, acommon.PayloadTypes.Logs, acommon.AttrsSchema64, func(b *builder.RecordBuilderExt) acommon.RelatedRecordBuilder {
		return acommon.NewAttrs64Builder(b, acommon.PayloadTypes.LogRecordAttrs, cfg.Attrs64.Log.Sorter)
	}, observer)

	return &RelatedData64{
		relatedRecordsManager: rr,
		attrsBuilders: &Attrs64Builders{
			resource:  resource.(*acommon.Attrs64Builder),
			scope:     scope.(*acommon.Attrs64Builder),
			logRecord: logRecord.(*acommon.Attrs64Builder),
		},
	}
}

func (r *RelatedData64) Schemas() []acommon.SchemaWithPayload {
	return r.relatedRecordsManager.Schemas()
}

func (r *RelatedData64) Release() {
	r.relatedRecordsManager.Release()
}

func (r *RelatedData64) AttrsBuilders() *Attrs64Builders {
	return r.attrsBuilders
}

func (r *RelatedData64) RecordBuilderExt(payloadType *acommon.PayloadType) *builder.RecordBuilderExt {
	return r.relatedRecordsManager.RecordBuilderExt(payloadType)
}

func (r *RelatedData64) Reset() {
	r.relatedRecordsManager.Reset()
}

func (r *RelatedData64) BuildRecordMessages() ([]*record_message.RecordMessage, error) {
	return r.relatedRecordsManager.BuildRecordMessages()
}

type Attrs64Builders struct {
	resource  *acommon.Attrs64Builder
	scope     *acommon.Attrs64Builder
	logRecord *acommon.Attrs64Builder
}

func (ab *Attrs64Builders) Resource() *acommon.Attrs64Builder {
	return ab.resource
}

func (ab *Attrs64Builders) Scope() *acommon.Attrs64Builder {
	return ab.scope
}

func (ab *Attrs64Builders) LogRecord() *acommon.Attrs64Builder {
	return ab.logRecord
}

// Logs64Builder is a helper to build a list of resource logs. It differs from LogsBuilder in that it
// uses uint64 instead of uint16 for ID fields.
type Logs64Builder struct {
	released bool

	builder *builder.RecordBuilderExt // Record builder

	rb    *acommon.Resource64Builder      // `resource` builder
	scb   *acommon.Scope64Builder         // `scope` builder
	sschb *builder.StringBuilder          // scope `schema_url` builder
	ib    *builder.Uint64DeltaBuilder     //  id builder
	tub   *builder.TimestampBuilder       // `time_unix_nano` builder
	otub  *builder.TimestampBuilder       // `observed_time_unix_nano` builder
	tidb  *builder.FixedSizeBinaryBuilder // `trace_id` builder
	sidb  *builder.FixedSizeBinaryBuilder // `span_id` builder
	snb   *builder.Int32Builder           // `severity_number` builder
	stb   *builder.StringBuilder          // `severity_text` builder

	bodyb *builder.StructBuilder // `body` builder
	typeb *builder.Uint8Builder
	strb  *builder.StringBuilder
	i64b  *builder.Int64Builder
	f64b  *builder.Float64Builder
	boolb *builder.BooleanBuilder
	binb  *builder.BinaryBuilder
	serb  *builder.BinaryBuilder

	dacb *builder.Uint32Builder // `dropped_attributes_count` builder
	fb   *builder.Uint32Builder // `flags` builder

	optimizer *LogsOptimizer
	analyzer  *LogsAnalyzer

	relatedData *RelatedData64
}

func NewLogs64Builder(
	recordBuilder *builder.RecordBuilderExt,
	cfg *Config,
	stats *stats.ProducerStats,
	observer observer.ProducerObserver,
) (*Logs64Builder, error) {
	var optimizer *LogsOptimizer
	var analyzer *LogsAnalyzer

	relatedData := NewRelatedData64(cfg, stats, observer)

	if stats.SchemaStats {
		optimizer = NewLogsOptimizer(cfg.Log.Sorter)
		analyzer = NewLogsAnalyzer()
	} else {
		optimizer = NewLogsOptimizer(cfg.Log.Sorter)
	}

	b := &Logs64Builder{
		builder:     recordBuilder,
		optimizer:   optimizer,
		analyzer:    analyzer,
		relatedData: relatedData,
	}
	b.init()

	return b, nil
}

func (b *Logs64Builder) init() {
	ib := b.builder.Uint64DeltaBuilder(constants.ID)
	// As the attributes are sorted before insertion, the delta between two
	// consecutive attributes ID should always be <=1.
	ib.SetMaxDelta(1)

	b.ib = ib
	b.rb = acommon.Resource64BuilderFrom(b.builder.StructBuilder(constants.Resource))
	b.scb = acommon.Scope64BuilderFrom(b.builder.StructBuilder(constants.Scope))
	b.sschb = b.builder.StringBuilder(constants.SchemaUrl)

	b.tub = b.builder.TimestampBuilder(constants.TimeUnixNano)
	b.otub = b.builder.TimestampBuilder(constants.ObservedTimeUnixNano)
	b.tidb = b.builder.FixedSizeBinaryBuilder(constants.TraceId)
	b.sidb = b.builder.FixedSizeBinaryBuilder(constants.SpanId)
	b.snb = b.builder.Int32Builder(constants.SeverityNumber)
	b.stb = b.builder.StringBuilder(constants.SeverityText)

	b.bodyb = b.builder.StructBuilder(constants.Body)
	b.typeb = b.bodyb.Uint8Builder(constants.BodyType)
	b.strb = b.bodyb.StringBuilder(constants.BodyStr)
	b.i64b = b.bodyb.Int64Builder(constants.BodyInt)
	b.f64b = b.bodyb.Float64Builder(constants.BodyDouble)
	b.boolb = b.bodyb.BooleanBuilder(constants.BodyBool)
	b.binb = b.bodyb.BinaryBuilder(constants.BodyBytes)
	b.serb = b.bodyb.BinaryBuilder(constants.BodySer)

	b.dacb = b.builder.Uint32Builder(constants.DroppedAttributesCount)
	b.fb = b.builder.Uint32Builder(constants.Flags)
}

func (b *Logs64Builder) RelatedData() RelatedDataInterface {
	return b.relatedData
}

// Build builds an Arrow Record from the builder.
//
// Once the array is no longer needed, Release() must be called to free the
// memory allocated by the record.
func (b *Logs64Builder) Build() (record arrow.Record, err error) {
	if b.released {
		return nil, werror.Wrap(acommon.ErrBuilderAlreadyReleased)
	}

	record, err = b.builder.NewRecord()
	if err != nil {
		// Only error is if schema needs updating; if that's the case, re-initialize.
		b.init()
	}

	return
}

// Append appends a new set of resource logs to the builder.
func (b *Logs64Builder) Append(logs plog.Logs) (err error) {
	if b.released {
		return werror.Wrap(acommon.ErrBuilderAlreadyReleased)
	}

	optimLogs := b.optimizer.Optimize(logs)
	if b.analyzer != nil {
		b.analyzer.Analyze(optimLogs)
		b.analyzer.ShowStats("")
	}

	attrsAccu := b.relatedData.AttrsBuilders().LogRecord().Accumulator()

	logID := uint64(0)
	resLogID := -1
	scopeLogID := -1
	resID := uint64(math.MaxUint64)
	scopeID := uint64(math.MaxUint64)

	b.builder.Reserve(len(optimLogs.Logs))

	for _, logRec := range optimLogs.Logs {
		log := logRec.Log
		logAttrs := log.Attributes()

		ID := logID

		if logAttrs.Len() == 0 {
			b.ib.AppendNull()
		} else {
			b.ib.Append(ID)
			logID++
		}

		// === Process resource and schema URL ===
		resAttrs := logRec.ResScope.Resource.Attributes()
		if resLogID != logRec.ResScope.ResourceLogsID {
			// New resource ID detected =>
			// - Increment the resource ID
			// - Append the resource attributes to the resource attributes accumulator
			resLogID = logRec.ResScope.ResourceLogsID
			resID++
			err = b.relatedData.AttrsBuilders().Resource().Accumulator().
				AppendWithID(resID, resAttrs)
			if err != nil {
				return werror.Wrap(err)
			}
		}
		// Check resID validity
		if resID >= math.MaxUint64 {
			return werror.WrapWithContext(acommon.ErrInvalidResourceID, map[string]interface{}{
				"resource_id": resID,
			})
		}
		// Append the resource schema URL if exists
		if err = b.rb.Append(resID, logRec.ResScope.Resource, logRec.ResScope.ResourceSchemaUrl); err != nil {
			return werror.Wrap(err)
		}

		// === Process scope and schema URL ===
		scopeAttrs := logRec.ResScope.Scope.Attributes()
		if scopeLogID != logRec.ResScope.ScopeLogsID {
			// New scope ID detected =>
			// - Increment the scope ID
			// - Append the scope attributes to the scope attributes accumulator
			scopeLogID = logRec.ResScope.ScopeLogsID
			scopeID++
			err = b.relatedData.AttrsBuilders().scope.Accumulator().
				AppendWithID(scopeID, scopeAttrs)
			if err != nil {
				return werror.Wrap(err)
			}
		}
		// Check scopeID validity
		if scopeID >= math.MaxUint64 {
			return werror.WrapWithContext(acommon.ErrInvalidScopeID, map[string]interface{}{
				"scope_id": scopeID,
			})
		}
		// Append the scope name, version, and schema URL (if exists)
		if err = b.scb.Append(scopeID, logRec.ResScope.Scope); err != nil {
			return werror.Wrap(err)
		}
		b.sschb.AppendNonEmpty(logRec.ResScope.ScopeSchemaUrl)

		// === Process log record ===
		b.tub.Append(arrow.Timestamp(log.Timestamp()))
		b.otub.Append(arrow.Timestamp(log.ObservedTimestamp()))
		tib := log.TraceID()
		b.tidb.Append(tib[:])
		sib := log.SpanID()
		b.sidb.Append(sib[:])
		b.snb.AppendNonZero(int32(log.SeverityNumber()))
		b.stb.AppendNonEmpty(log.SeverityText())

		// Log record body
		body := log.Body()
		switch body.Type() {
		case pcommon.ValueTypeStr:
			err = b.bodyb.Append(body, func() error {
				b.typeb.Append(uint8(pcommon.ValueTypeStr))
				b.strb.Append(body.Str())
				b.i64b.AppendNull()
				b.f64b.AppendNull()
				b.boolb.AppendNull()
				b.binb.AppendNull()
				b.serb.AppendNull()
				return nil
			})
			if err != nil {
				return werror.Wrap(err)
			}
		case pcommon.ValueTypeInt:
			err = b.bodyb.Append(body, func() error {
				b.typeb.Append(uint8(pcommon.ValueTypeInt))
				b.i64b.Append(body.Int())
				b.strb.AppendNull()
				b.f64b.AppendNull()
				b.boolb.AppendNull()
				b.binb.AppendNull()
				b.serb.AppendNull()
				return nil
			})
			if err != nil {
				return werror.Wrap(err)
			}
		case pcommon.ValueTypeDouble:
			err = b.bodyb.Append(body, func() error {
				b.typeb.Append(uint8(pcommon.ValueTypeDouble))
				b.f64b.Append(body.Double())
				b.strb.AppendNull()
				b.i64b.AppendNull()
				b.boolb.AppendNull()
				b.binb.AppendNull()
				b.serb.AppendNull()
				return nil
			})
			if err != nil {
				return werror.Wrap(err)
			}
		case pcommon.ValueTypeBool:
			err = b.bodyb.Append(body, func() error {
				b.typeb.Append(uint8(pcommon.ValueTypeBool))
				b.boolb.Append(body.Bool())
				b.strb.AppendNull()
				b.i64b.AppendNull()
				b.f64b.AppendNull()
				b.binb.AppendNull()
				b.serb.AppendNull()
				return nil
			})
			if err != nil {
				return werror.Wrap(err)
			}
		case pcommon.ValueTypeBytes:
			err = b.bodyb.Append(body, func() error {
				b.typeb.Append(uint8(pcommon.ValueTypeBytes))
				b.binb.Append(body.Bytes().AsRaw())
				b.strb.AppendNull()
				b.i64b.AppendNull()
				b.f64b.AppendNull()
				b.boolb.AppendNull()
				b.serb.AppendNull()
				return nil
			})
			if err != nil {
				return werror.Wrap(err)
			}
		case pcommon.ValueTypeSlice:
			cborData, err := common.Serialize(&body)
			if err != nil {
				return werror.Wrap(err)
			}
			err = b.bodyb.Append(body, func() error {
				b.typeb.Append(uint8(pcommon.ValueTypeSlice))
				b.serb.Append(cborData)
				b.strb.AppendNull()
				b.i64b.AppendNull()
				b.f64b.AppendNull()
				b.boolb.AppendNull()
				b.binb.AppendNull()
				return nil
			})
			if err != nil {
				return werror.Wrap(err)
			}
		case pcommon.ValueTypeMap:
			cborData, err := common.Serialize(&body)
			if err != nil {
				return werror.Wrap(err)
			}
			err = b.bodyb.Append(body, func() error {
				b.typeb.Append(uint8(pcommon.ValueTypeMap))
				b.serb.Append(cborData)
				b.strb.AppendNull()
				b.i64b.AppendNull()
				b.f64b.AppendNull()
				b.boolb.AppendNull()
				b.binb.AppendNull()
				return nil
			})
			if err != nil {
				return werror.Wrap(err)
			}
		case pcommon.ValueTypeEmpty:
			b.bodyb.AppendNull()
		}

		// Log record attributes
		if logAttrs.Len() > 0 {
			err := attrsAccu.AppendWithID(ID, log.Attributes())
			if err != nil {
				return werror.Wrap(err)
			}
		}

		b.dacb.Append(log.DroppedAttributesCount())

		b.fb.Append(uint32(log.Flags()))
	}
	return nil
}

// Release releases the memory allocated by the builder.
func (b *Logs64Builder) Release() {
	if !b.released {
		// b.builder is a shared resource => not released here

		b.relatedData.Release()
		b.released = true
	}
}

func (b *Logs64Builder) ShowSchema() {
	b.builder.ShowSchema()
}
