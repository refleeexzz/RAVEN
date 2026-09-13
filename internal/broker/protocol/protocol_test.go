package protocol

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		frame Frame
	}{
		{"empty payload", Frame{Opcode: OpListTopics, CorrelationID: 0, Payload: nil}},
		{"json payload", Frame{Opcode: OpJoinGroup, CorrelationID: 42, Payload: []byte(`{"group":"g"}`)}},
		{"max correlation", Frame{Opcode: OpHeartbeat, CorrelationID: ^uint64(0), Payload: []byte{0x00, 0xff, 0x10}}},
		{"error frame", Frame{Opcode: OpError, CorrelationID: 7, Payload: []byte(`{"code":"BROKER_BUSY","message":"queue full"}`)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			if err := WriteFrame(&buf, &tc.frame); err != nil {
				t.Fatalf("WriteFrame: %v", err)
			}
			got, err := ReadFrame(&buf)
			if err != nil {
				t.Fatalf("ReadFrame: %v", err)
			}
			if got.Opcode != tc.frame.Opcode {
				t.Errorf("opcode: got %v, want %v", got.Opcode, tc.frame.Opcode)
			}
			if got.CorrelationID != tc.frame.CorrelationID {
				t.Errorf("correlation id: got %d, want %d", got.CorrelationID, tc.frame.CorrelationID)
			}
			if !bytes.Equal(got.Payload, tc.frame.Payload) && len(got.Payload)+len(tc.frame.Payload) > 0 {
				t.Errorf("payload: got %q, want %q", got.Payload, tc.frame.Payload)
			}
		})
	}
}

func TestReadFrameRejectsOversize(t *testing.T) {
	t.Parallel()
	// Announce 5 MiB, which is over the 4 MiB cap.
	head := []byte{0x00, 0x50, 0x00, 0x00}
	_, err := ReadFrame(bytes.NewReader(head))
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("got %v, want ErrFrameTooLarge", err)
	}
}

func TestWriteFrameRejectsOversize(t *testing.T) {
	t.Parallel()
	f := &Frame{Opcode: OpProduce, Payload: make([]byte, MaxPayloadSize+1)}
	var buf bytes.Buffer
	if err := WriteFrame(&buf, f); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("got %v, want ErrFrameTooLarge", err)
	}
}

func TestReadFrameTruncated(t *testing.T) {
	t.Parallel()
	// Valid header announcing 100 payload bytes, but stream ends early.
	var buf bytes.Buffer
	_ = WriteFrame(&buf, &Frame{Opcode: OpFetch, CorrelationID: 1, Payload: make([]byte, 100)})
	data := buf.Bytes()[:buf.Len()-10]
	_, err := ReadFrame(bytes.NewReader(data))
	if err == nil || errors.Is(err, io.EOF) && err != io.ErrUnexpectedEOF {
		t.Fatalf("got %v, want unexpected EOF", err)
	}
}

func TestErrorFrameRoundTrip(t *testing.T) {
	t.Parallel()
	f := ErrorFrame(99, NewError(CodeBrokerBusy, "queue full"))
	if f.Opcode != OpError || f.CorrelationID != 99 {
		t.Fatalf("bad error frame header: %+v", f)
	}
	e := DecodeErrorFrame(f.Payload)
	if e.Code != CodeBrokerBusy || e.Message != "queue full" {
		t.Fatalf("decoded %+v", e)
	}

	// Plain errors become INTERNAL.
	f2 := ErrorFrame(1, errors.New("boom"))
	if DecodeErrorFrame(f2.Payload).Code != CodeInternal {
		t.Fatalf("plain error should map to INTERNAL")
	}
}

func TestProduceRequestRoundTrip(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		req  ProduceRequest
	}{
		{
			name: "single record explicit partition",
			req: ProduceRequest{
				Topic: "jobs", Partition: 2,
				Records: []Message{{Key: []byte("k"), Value: []byte("v"), Headers: []Header{{Key: "h1", Value: []byte("x")}}}},
			},
		},
		{
			name: "batch auto partition",
			req: ProduceRequest{
				Topic: "jobs", Partition: -1,
				Records: []Message{
					{Value: []byte("a")},
					{Key: []byte("key"), Value: []byte("b")},
					{Value: nil, Headers: nil},
				},
			},
		},
		{
			name: "no headers empty key",
			req:  ProduceRequest{Topic: "t", Partition: 0, Records: []Message{{Value: []byte{0x1, 0x2, 0x3}}}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := DecodeProduceRequest(EncodeProduceRequest(&tc.req))
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if got.Topic != tc.req.Topic || got.Partition != tc.req.Partition {
				t.Fatalf("header mismatch: %+v vs %+v", got, tc.req)
			}
			if len(got.Records) != len(tc.req.Records) {
				t.Fatalf("record count: got %d want %d", len(got.Records), len(tc.req.Records))
			}
			for i := range tc.req.Records {
				want, g := tc.req.Records[i], got.Records[i]
				if !bytes.Equal(want.Key, g.Key) || !bytes.Equal(want.Value, g.Value) {
					t.Errorf("record %d: got k=%q v=%q want k=%q v=%q", i, g.Key, g.Value, want.Key, want.Value)
				}
				if len(want.Headers) != len(g.Headers) {
					t.Errorf("record %d: header count got %d want %d", i, len(g.Headers), len(want.Headers))
				}
			}
		})
	}
}

func TestProduceResponseRoundTrip(t *testing.T) {
	t.Parallel()
	resp := ProduceResponse{Results: []ProduceResult{
		{Partition: 0, Offset: 10},
		{Partition: 2, Offset: 5},
		{Partition: 0, Offset: 11},
	}}
	got, err := DecodeProduceResponse(EncodeProduceResponse(&resp))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Results) != len(resp.Results) {
		t.Fatalf("count mismatch")
	}
	for i := range resp.Results {
		if got.Results[i] != resp.Results[i] {
			t.Errorf("result %d: got %+v want %+v", i, got.Results[i], resp.Results[i])
		}
	}
}

func TestFetchRoundTrip(t *testing.T) {
	t.Parallel()
	req := FetchRequest{
		Topic: "jobs", Partition: 1, Offset: 42,
		MaxRecords: 100, MaxBytes: 1 << 20,
		Group: "workers", MemberID: "m-1", Generation: 7,
	}
	gotReq, err := DecodeFetchRequest(EncodeFetchRequest(&req))
	if err != nil {
		t.Fatalf("decode req: %v", err)
	}
	if *gotReq != req {
		t.Fatalf("request mismatch:\n got %+v\nwant %+v", *gotReq, req)
	}

	resp := FetchResponse{
		HighWatermark: 50,
		Records: []FetchedMessage{
			{Offset: 42, Timestamp: 1700000000000, Key: []byte("k"), Value: []byte("v"), Headers: []Header{{Key: "h", Value: []byte("z")}}},
			{Offset: 43, Timestamp: 1700000000001, Value: []byte("v2")},
		},
	}
	gotResp, err := DecodeFetchResponse(EncodeFetchResponse(&resp))
	if err != nil {
		t.Fatalf("decode resp: %v", err)
	}
	if gotResp.HighWatermark != resp.HighWatermark || len(gotResp.Records) != len(resp.Records) {
		t.Fatalf("response mismatch: %+v", gotResp)
	}
	if gotResp.Records[0].Offset != 42 || gotResp.Records[0].Headers[0].Key != "h" {
		t.Fatalf("first record mismatch: %+v", gotResp.Records[0])
	}
}

func TestDecodeTruncatedPayloads(t *testing.T) {
	t.Parallel()
	// Every truncation must return an error, never panic.
	full := EncodeProduceRequest(&ProduceRequest{
		Topic: "jobs", Partition: -1,
		Records: []Message{{Key: []byte("k"), Value: []byte("v"), Headers: []Header{{Key: "h", Value: []byte("x")}}}},
	})
	for i := 0; i < len(full); i++ {
		if _, err := DecodeProduceRequest(full[:i]); err == nil {
			t.Fatalf("truncated produce (%d bytes) decoded without error", i)
		}
	}

	fullFetch := EncodeFetchResponse(&FetchResponse{
		HighWatermark: 3,
		Records:       []FetchedMessage{{Offset: 1, Timestamp: 2, Key: []byte("k"), Value: []byte("v")}},
	})
	for i := 0; i < len(fullFetch); i++ {
		if _, err := DecodeFetchResponse(fullFetch[:i]); err == nil {
			t.Fatalf("truncated fetch response (%d bytes) decoded without error", i)
		}
	}
}

func TestJSONControlRoundTrip(t *testing.T) {
	t.Parallel()
	join := JoinGroupRequest{Group: "workers", MemberID: "m-1", Topics: []string{"jobs", "jobs.retry"}}
	var decoded JoinGroupRequest
	if err := unmarshalJSON(marshalJSON(&join), &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Group != join.Group || decoded.MemberID != join.MemberID || len(decoded.Topics) != 2 {
		t.Fatalf("mismatch: %+v", decoded)
	}
}
