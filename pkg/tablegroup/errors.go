// Copyright 2026 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tablegroup

import (
	stderrors "errors"
	"fmt"

	"github.com/gogo/protobuf/proto"

	"github.com/pingcap/kvproto/pkg/table_grouppb"
)

// Error is a Table Group domain error with stable machine-readable detail.
type Error struct {
	detail *table_grouppb.TableGroupError
}

// Error implements error. The message is diagnostic only.
func (e *Error) Error() string {
	if e == nil || e.detail == nil {
		return "table group error"
	}
	if e.detail.GetMessage() != "" {
		return e.detail.GetMessage()
	}
	return e.detail.GetCode().String()
}

// Detail returns a clone of the structured error detail.
func (e *Error) Detail() *table_grouppb.TableGroupError {
	if e == nil || e.detail == nil {
		return nil
	}
	return proto.Clone(e.detail).(*table_grouppb.TableGroupError)
}

// ErrorDetail extracts and clones a Table Group error detail.
func ErrorDetail(err error) *table_grouppb.TableGroupError {
	var domainErr *Error
	if !stderrors.As(err, &domainErr) {
		return nil
	}
	return domainErr.Detail()
}

func newError(code table_grouppb.TableGroupErrorCode, message string) *Error {
	return &Error{detail: &table_grouppb.TableGroupError{Code: code, Message: message}}
}

func invalidArgument(message string) *Error {
	return newError(table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_INVALID_ARGUMENT, message)
}

func notFound(identity *table_grouppb.TableGroupIdentity) *Error {
	err := newError(table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_NOT_FOUND, "table group not found")
	err.detail.Identity = cloneIdentity(identity)
	return err
}

func alreadyExists(message string, identity *table_grouppb.TableGroupIdentity) *Error {
	err := newError(table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_ALREADY_EXISTS, message)
	err.detail.Identity = cloneIdentity(identity)
	return err
}

func staleMetadata(identity *table_grouppb.TableGroupIdentity, expected, actual uint64) *Error {
	err := newError(table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_STALE_METADATA_VERSION,
		fmt.Sprintf("stale table group metadata version: expected %d, actual %d", expected, actual))
	err.detail.Identity = cloneIdentity(identity)
	err.detail.ExpectedMetadataVersion = expected
	err.detail.ActualMetadataVersion = actual
	return err
}

func operationConflict(identity *table_grouppb.TableGroupIdentity, token []byte, message string) *Error {
	err := newError(table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_OPERATION_CONFLICT, message)
	err.detail.Identity = cloneIdentity(identity)
	err.detail.OperationToken = append([]byte(nil), token...)
	return err
}

func invalidState(identity *table_grouppb.TableGroupIdentity, message string) *Error {
	err := newError(table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_INVALID_STATE, message)
	err.detail.Identity = cloneIdentity(identity)
	return err
}

func keyspaceMismatch(identity *table_grouppb.TableGroupIdentity, message string) *Error {
	err := newError(table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_KEYSPACE_MISMATCH, message)
	err.detail.Identity = cloneIdentity(identity)
	return err
}

func regionMismatch(identity *table_grouppb.TableGroupIdentity, message string) *Error {
	err := newError(table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_REGION_MISMATCH, message)
	err.detail.Identity = cloneIdentity(identity)
	return err
}

func epochMismatch(identity *table_grouppb.TableGroupIdentity, message string) *Error {
	err := newError(table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_EPOCH_MISMATCH, message)
	err.detail.Identity = cloneIdentity(identity)
	return err
}

func membershipConflict(identity *table_grouppb.TableGroupIdentity, message string) *Error {
	err := newError(table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_MEMBERSHIP_CONFLICT, message)
	err.detail.Identity = cloneIdentity(identity)
	return err
}

func splitForbidden(
	identity *table_grouppb.TableGroupIdentity,
	source table_grouppb.SplitSource,
	reason table_grouppb.SplitRejectionReason,
	message string,
) *Error {
	err := newError(table_grouppb.TableGroupErrorCode_TABLE_GROUP_ERROR_CODE_SPLIT_FORBIDDEN, message)
	err.detail.Identity = cloneIdentity(identity)
	err.detail.SplitSource = source
	err.detail.SplitRejectionReason = reason
	tableGroupSplitRejectedCounter.WithLabelValues(source.String(), reason.String()).Inc()
	return err
}

func cloneIdentity(identity *table_grouppb.TableGroupIdentity) *table_grouppb.TableGroupIdentity {
	if identity == nil {
		return nil
	}
	return proto.Clone(identity).(*table_grouppb.TableGroupIdentity)
}
