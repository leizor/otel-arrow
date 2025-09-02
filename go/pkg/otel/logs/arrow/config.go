/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package arrow

// General configuration for logs. This configuration defines the different
// sorters used for attributes, log records, ... and the encoding used for
// parent IDs.

import (
	cfg "github.com/open-telemetry/otel-arrow/go/pkg/config"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/arrow"
)

type (
	Config struct {
		Global *cfg.Config

		Log     *LogConfig
		Attrs   *AttrsConfig
		Attrs64 *Attrs64Config
	}

	AttrsConfig struct {
		Resource *arrow.Attrs16Config
		Scope    *arrow.Attrs16Config
		Log      *arrow.Attrs16Config
	}

	Attrs64Config struct {
		Resource *arrow.Attrs64Config
		Scope    *arrow.Attrs64Config
		Log      *arrow.Attrs64Config
	}

	LogConfig struct {
		Sorter LogSorter
	}
)

func DefaultConfig() *Config {
	return NewConfig(cfg.DefaultConfig())
}

func NewConfig(globalConf *cfg.Config) *Config {
	return &Config{
		Global: globalConf,
		Log: &LogConfig{
			Sorter: SortLogsByResourceLogsIDScopeLogsIDTraceID(),
		},
		Attrs: &AttrsConfig{
			Resource: &arrow.Attrs16Config{
				Sorter: arrow.SortAttrs16ByTypeKeyValueParentId(),
			},
			Scope: &arrow.Attrs16Config{
				Sorter: arrow.SortAttrs16ByTypeKeyValueParentId(),
			},
			Log: &arrow.Attrs16Config{
				Sorter: arrow.SortAttrs16ByTypeKeyValueParentId(),
			},
		},
		Attrs64: &Attrs64Config{
			Resource: &arrow.Attrs64Config{
				Sorter: arrow.SortAttrs64ByTypeKeyValueParentId(),
			},
			Scope: &arrow.Attrs64Config{
				Sorter: arrow.SortAttrs64ByTypeKeyValueParentId(),
			},
			Log: &arrow.Attrs64Config{
				Sorter: arrow.SortAttrs64ByTypeKeyValueParentId(),
			},
		},
	}
}

func NewNoSortConfig(globalConf *cfg.Config) *Config {
	return &Config{
		Global: globalConf,
		Log: &LogConfig{
			Sorter: UnsortedLogs(),
		},
		Attrs: &AttrsConfig{
			Resource: &arrow.Attrs16Config{
				Sorter: arrow.UnsortedAttrs16(),
			},
			Scope: &arrow.Attrs16Config{
				Sorter: arrow.UnsortedAttrs16(),
			},
			Log: &arrow.Attrs16Config{
				Sorter: arrow.UnsortedAttrs16(),
			},
		},
		Attrs64: &Attrs64Config{
			Resource: &arrow.Attrs64Config{
				Sorter: arrow.UnsortedAttrs64(),
			},
			Scope: &arrow.Attrs64Config{
				Sorter: arrow.UnsortedAttrs64(),
			},
			Log: &arrow.Attrs64Config{
				Sorter: arrow.UnsortedAttrs64(),
			},
		},
	}
}
