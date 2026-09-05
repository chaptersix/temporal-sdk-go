package internal

import (
	"context"
	iconverter "go.temporal.io/sdk/internal/converter"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservicemock/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	scheduleID = "some random schedule ID"
)

// schedule client test suite
type (
	scheduleClientTestSuite struct {
		suite.Suite
		mockCtrl      *gomock.Controller
		service       *workflowservicemock.MockWorkflowServiceClient
		client        Client
		dataConverter converter.DataConverter
	}
)

func TestScheduleClientSuite(t *testing.T) {
	suite.Run(t, new(scheduleClientTestSuite))
}

func (s *scheduleClientTestSuite) TestCreateAndDescribeScheduleActivity() {
	headerPayload, err := converter.GetDefaultDataConverter().ToPayload("header-value")
	s.NoError(err)
	options := ScheduleOptions{
		ID:   scheduleID,
		Spec: ScheduleSpec{CronExpressions: []string{"*"}},
		Action: &ScheduleActivityAction{
			ID: "activity-id", Activity: "activity-type", Args: []any{"argument"}, TaskQueue: taskqueue,
			StartToCloseTimeout: time.Minute, HeartbeatTimeout: time.Second, StartDelay: 2 * time.Second,
			StaticSummary: "summary", Priority: Priority{PriorityKey: 3},
			Header:                &commonpb.Header{Fields: map[string]*commonpb.Payload{"header": headerPayload}},
			TypedSearchAttributes: NewSearchAttributes(NewSearchAttributeKeyKeyword("CustomKeywordField").ValueSet("value")),
		},
		CustomOverlapPolicy: ScheduleOverlapPolicyBufferLatest,
	}
	s.service.EXPECT().CreateSchedule(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *workflowservice.CreateScheduleRequest, _ ...any) (*workflowservice.CreateScheduleResponse, error) {
			s.Equal(ScheduleOverlapPolicyBufferLatest, request.Schedule.Policies.GetCustomOverlapPolicy().GetName())
			activity := request.Schedule.Action.GetStartActivity()
			s.Equal("activity-id", activity.GetActivityId())
			s.Equal("activity-type", activity.GetActivityType().GetName())
			s.Equal(taskqueue, activity.GetTaskQueue().GetName())
			s.Equal(time.Minute, activity.GetStartToCloseTimeout().AsDuration())
			s.Equal(time.Second, activity.GetHeartbeatTimeout().AsDuration())
			s.Equal(2*time.Second, activity.GetStartDelay().AsDuration())
			s.Equal(int32(3), activity.GetPriority().GetPriorityKey())
			s.Equal(headerPayload, activity.GetHeader().GetFields()["header"])
			s.Contains(activity.GetSearchAttributes().GetIndexedFields(), "CustomKeywordField")
			var summary string
			s.NoError(converter.GetDefaultDataConverter().FromPayload(activity.GetUserMetadata().GetSummary(), &summary))
			s.Equal("summary", summary)
			return &workflowservice.CreateScheduleResponse{}, nil
		})

	_, err = s.client.ScheduleClient().Create(context.Background(), options)
	s.NoError(err)
}

func (s *scheduleClientTestSuite) TestDescribeAndUpdateScheduleActivity() {
	dc := converter.GetDefaultDataConverter()
	input, err := dc.ToPayloads("argument")
	s.NoError(err)
	header, err := dc.ToPayload("header-value")
	s.NoError(err)
	metadata, err := BuildUserMetadata("summary", "details", dc)
	s.NoError(err)
	action := &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartActivity{StartActivity: &schedulepb.StartActivityExecutionInfo{
		ActivityId: "activity-id", ActivityType: &commonpb.ActivityType{Name: "activity-type"}, TaskQueue: &taskqueuepb.TaskQueue{Name: taskqueue},
		StartToCloseTimeout: durationpb.New(time.Minute), Input: input, Header: &commonpb.Header{Fields: map[string]*commonpb.Payload{"header": header}},
		UserMetadata: metadata, Priority: &commonpb.Priority{PriorityKey: 4}, StartDelay: durationpb.New(time.Second),
	}}}
	describeResponse := &workflowservice.DescribeScheduleResponse{Schedule: &schedulepb.Schedule{
		Action: action, Spec: &schedulepb.ScheduleSpec{}, Policies: &schedulepb.SchedulePolicies{CustomOverlapPolicy: &schedulepb.CustomOverlapPolicy{Name: ScheduleOverlapPolicyBufferLatest}}, State: &schedulepb.ScheduleState{},
	}, Info: &schedulepb.ScheduleInfo{ActionKind: enumspb.EXECUTION_TYPE_ACTIVITY, ActionType: "activity-type"}}
	s.service.EXPECT().DescribeSchedule(gomock.Any(), gomock.Any(), gomock.Any()).Return(describeResponse, nil)
	s.service.EXPECT().UpdateSchedule(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *workflowservice.UpdateScheduleRequest, _ ...any) (*workflowservice.UpdateScheduleResponse, error) {
			updated := request.Schedule.Action.GetStartActivity()
			s.Equal("activity-id", updated.GetActivityId())
			s.Equal("activity-type", updated.GetActivityType().GetName())
			s.Equal(ScheduleOverlapPolicyBufferLatest, request.Schedule.Policies.GetCustomOverlapPolicy().GetName())
			s.Equal(header, updated.GetHeader().GetFields()["header"])
			return &workflowservice.UpdateScheduleResponse{}, nil
		})

	err = s.client.ScheduleClient().GetHandle(context.Background(), scheduleID).Update(context.Background(), ScheduleUpdateOptions{DoUpdate: func(input ScheduleUpdateInput) (*ScheduleUpdate, error) {
		activity, ok := input.Description.Schedule.Action.(*ScheduleActivityAction)
		s.True(ok)
		s.Equal("summary", activity.StaticSummary)
		s.Equal("details", activity.StaticDetails)
		s.Equal(time.Second, activity.StartDelay)
		s.Equal(enumspb.EXECUTION_TYPE_ACTIVITY, input.Description.Info.ActionKind)
		return &ScheduleUpdate{Schedule: &input.Description.Schedule}, nil
	}})
	s.NoError(err)
}

func (s *scheduleClientTestSuite) TestSerializeConflictingOverlapSelectors() {
	policy := &SchedulePolicies{Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, CustomOverlapPolicy: "unknown.policy"}
	pb, err := convertToPBSchedule(contextWithNewHeader(context.Background()), s.client.(*WorkflowClient), &Schedule{Action: &ScheduleActivityAction{Activity: "activity-type"}, Spec: &ScheduleSpec{}, Policy: policy, State: &ScheduleState{}})
	s.NoError(err)
	s.Equal(enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, pb.Policies.GetOverlapPolicy())
	s.Equal("unknown.policy", pb.Policies.GetCustomOverlapPolicy().GetName())
}

func (s *scheduleClientTestSuite) TestConvertScheduleActionResults() {
	activityResult := &commonpb.ActionExecutionResult{
		Execution: &commonpb.Execution{Type: enumspb.EXECUTION_TYPE_ACTIVITY, BusinessId: "activity-id", RunId: "run-id"},
		Status:    &commonpb.ActionExecutionResult_ActivityStatus{ActivityStatus: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED},
	}
	results := convertFromPBScheduleActionResultList([]*schedulepb.ScheduleActionResult{{ActionExecutionResult: activityResult}, {
		StartWorkflowResult: &commonpb.WorkflowExecution{WorkflowId: "workflow-id", RunId: "workflow-run"},
	}})
	s.Equal(&ScheduleExecution{Kind: enumspb.EXECUTION_TYPE_ACTIVITY, ID: "activity-id", RunID: "run-id"}, results[0].Execution)
	s.Equal(enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, results[0].ActivityStatus)
	s.True(results[0].CloseTime.IsZero())
	s.Equal(&ScheduleExecution{Kind: enumspb.EXECUTION_TYPE_WORKFLOW, ID: "workflow-id", RunID: "workflow-run"}, results[1].Execution)
}

func (s *scheduleClientTestSuite) TestCustomOverlapPolicyOverrides() {
	handle := s.client.ScheduleClient().GetHandle(context.Background(), scheduleID)
	s.service.EXPECT().PatchSchedule(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *workflowservice.PatchScheduleRequest, _ ...any) (*workflowservice.PatchScheduleResponse, error) {
			s.Equal(ScheduleOverlapPolicyBufferLatest, request.Patch.GetTriggerImmediately().GetCustomOverlapPolicy().GetName())
			return &workflowservice.PatchScheduleResponse{}, nil
		})
	s.NoError(handle.Trigger(context.Background(), ScheduleTriggerOptions{CustomOverlapPolicy: ScheduleOverlapPolicyBufferLatest}))

	s.service.EXPECT().PatchSchedule(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *workflowservice.PatchScheduleRequest, _ ...any) (*workflowservice.PatchScheduleResponse, error) {
			s.Equal(ScheduleOverlapPolicyBufferLatest, request.Patch.GetBackfillRequest()[0].GetCustomOverlapPolicy().GetName())
			return &workflowservice.PatchScheduleResponse{}, nil
		})
	s.NoError(handle.Backfill(context.Background(), ScheduleBackfillOptions{Backfill: []ScheduleBackfill{{
		Start: time.Now().Add(-time.Hour), End: time.Now(), CustomOverlapPolicy: ScheduleOverlapPolicyBufferLatest,
	}}}))
}

func (s *scheduleClientTestSuite) SetupTest() {
	s.mockCtrl = gomock.NewController(s.T())
	s.service = workflowservicemock.NewMockWorkflowServiceClient(s.mockCtrl)
	s.service.EXPECT().GetSystemInfo(gomock.Any(), gomock.Any(), gomock.Any()).Return(&workflowservice.GetSystemInfoResponse{}, nil).AnyTimes()
	s.client = NewServiceClient(s.service, nil, ClientOptions{})
	s.dataConverter = converter.GetDefaultDataConverter()
}

func (s *scheduleClientTestSuite) TearDownTest() {
	s.mockCtrl.Finish() // assert mock’s expectations
}

func (s *scheduleClientTestSuite) TestCreateScheduleClient() {
	wf := func(ctx Context) string {
		panic("this is just a stub")
	}
	options := ScheduleOptions{
		ID: scheduleID,
		Spec: ScheduleSpec{
			CronExpressions: []string{"*"},
		},
		Action: &ScheduleWorkflowAction{
			Workflow:                 wf,
			ID:                       workflowID,
			TaskQueue:                taskqueue,
			WorkflowExecutionTimeout: timeoutInSeconds,
			WorkflowTaskTimeout:      timeoutInSeconds,
		},
	}
	createResp := &workflowservice.CreateScheduleResponse{}
	s.service.EXPECT().CreateSchedule(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(_ any, req *workflowservice.CreateScheduleRequest, _ ...any) {
			s.Nil(req.Schedule.Policies.CatchupWindow)
		}).
		Return(createResp, nil).
		Times(1)

	scheduleHandle, err := s.client.ScheduleClient().Create(context.Background(), options)
	s.Nil(err)
	s.Equal(scheduleHandle.GetID(), scheduleID)
}

func (s *scheduleClientTestSuite) TestCreateScheduleNoID() {
	wf := func(ctx Context) string {
		panic("this is just a stub")
	}
	options := ScheduleOptions{
		Spec: ScheduleSpec{
			CronExpressions: []string{"*"},
		},
		Action: &ScheduleWorkflowAction{
			Workflow:                 wf,
			ID:                       workflowID,
			TaskQueue:                taskqueue,
			WorkflowExecutionTimeout: timeoutInSeconds,
			WorkflowTaskTimeout:      timeoutInSeconds,
		},
	}

	_, err := s.client.ScheduleClient().Create(context.Background(), options)
	s.NotNil(err)
}

func (s *scheduleClientTestSuite) TestCreateScheduleWithMemoAndSearchAttr() {
	memo := map[string]any{
		"testMemo": "memo value",
	}
	searchAttributes := map[string]any{
		"testAttr": "attr value",
	}

	wf := func(ctx Context) string {
		panic("this is just a stub")
	}

	options := ScheduleOptions{
		ID: scheduleID,
		Spec: ScheduleSpec{
			CronExpressions: []string{"*"},
		},
		Action: &ScheduleWorkflowAction{
			Workflow:                 wf,
			ID:                       "wid",
			TaskQueue:                taskqueue,
			WorkflowExecutionTimeout: timeoutInSeconds,
			WorkflowTaskTimeout:      timeoutInSeconds,
		},
		Memo:             memo,
		SearchAttributes: searchAttributes,
	}
	createResp := &workflowservice.CreateScheduleResponse{}

	s.service.EXPECT().CreateSchedule(gomock.Any(), gomock.Any(), gomock.Any()).Return(createResp, nil).
		Do(func(_ any, req *workflowservice.CreateScheduleRequest, _ ...any) {
			var resultMemo, resultAttr string
			// verify the schedules memo and search attributes
			err := converter.GetDefaultDataConverter().FromPayload(req.Memo.Fields["testMemo"], &resultMemo)
			s.NoError(err)
			s.Equal("memo value", resultMemo)

			err = converter.GetDefaultDataConverter().FromPayload(req.SearchAttributes.IndexedFields["testAttr"], &resultAttr)
			s.NoError(err)
			s.Equal("attr value", resultAttr)
		})
	_, _ = s.client.ScheduleClient().Create(context.Background(), options)
}

func getListSchedulesRequest() *workflowservice.ListSchedulesRequest {
	request := &workflowservice.ListSchedulesRequest{
		Namespace: DefaultNamespace,
	}

	return request
}

// ScheduleIterator

func (s *scheduleClientTestSuite) TestScheduleIterator_NoError() {
	request1 := getListSchedulesRequest()
	response1 := &workflowservice.ListSchedulesResponse{
		Schedules: []*schedulepb.ScheduleListEntry{
			{
				ScheduleId: "",
			},
		},
		NextPageToken: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	}
	request2 := getListSchedulesRequest()
	request2.NextPageToken = response1.NextPageToken
	response2 := &workflowservice.ListSchedulesResponse{
		Schedules: []*schedulepb.ScheduleListEntry{
			{
				ScheduleId: "",
			},
		},
		NextPageToken: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	}

	request3 := getListSchedulesRequest()
	request3.NextPageToken = response2.NextPageToken
	response3 := &workflowservice.ListSchedulesResponse{
		Schedules: []*schedulepb.ScheduleListEntry{
			{
				ScheduleId: "",
			},
		},
		NextPageToken: nil,
	}

	s.service.EXPECT().ListSchedules(gomock.Any(), request1, gomock.Any()).Return(response1, nil).Times(1)
	s.service.EXPECT().ListSchedules(gomock.Any(), request2, gomock.Any()).Return(response2, nil).Times(1)
	s.service.EXPECT().ListSchedules(gomock.Any(), request3, gomock.Any()).Return(response3, nil).Times(1)

	var events []*ScheduleListEntry
	iter, _ := s.client.ScheduleClient().List(context.Background(), ScheduleListOptions{})
	for iter.HasNext() {
		event, err := iter.Next()
		s.Nil(err)
		events = append(events, event)
	}
	s.Equal(3, len(events))
}

func (s *scheduleClientTestSuite) TestIteratorError() {
	request1 := getListSchedulesRequest()
	response1 := &workflowservice.ListSchedulesResponse{
		Schedules: []*schedulepb.ScheduleListEntry{
			{
				ScheduleId: "",
			},
		},
		NextPageToken: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	}
	request2 := getListSchedulesRequest()
	request2.NextPageToken = response1.NextPageToken

	s.service.EXPECT().ListSchedules(gomock.Any(), request1, gomock.Any()).Return(response1, nil).Times(1)

	iter, _ := s.client.ScheduleClient().List(context.Background(), ScheduleListOptions{})

	s.True(iter.HasNext())
	event, err := iter.Next()
	s.NotNil(event)
	s.Nil(err)

	s.service.EXPECT().ListSchedules(gomock.Any(), request2, gomock.Any()).Return(nil, serviceerror.NewNotFound("")).Times(1)

	s.True(iter.HasNext())
	event, err = iter.Next()
	s.Nil(event)
	s.NotNil(err)
}

func (s *scheduleClientTestSuite) TestCreateScheduleWorkflowMemoDataConverter() {
	testFn := func() {
		dc := iconverter.NewTestDataConverter()
		s.client = NewServiceClient(s.service, nil, ClientOptions{DataConverter: dc})

		memo := map[string]any{
			"testMemo": "memo value",
		}
		wf := func(ctx Context) string { panic("this is just a stub") }

		options := ScheduleOptions{
			ID: scheduleID,
			Spec: ScheduleSpec{
				CronExpressions: []string{"*"},
			},
			Action: &ScheduleWorkflowAction{
				Workflow:                 wf,
				ID:                       workflowID,
				TaskQueue:                taskqueue,
				WorkflowExecutionTimeout: timeoutInSeconds,
				WorkflowTaskTimeout:      timeoutInSeconds,
				Memo:                     memo,
			},
		}
		createResp := &workflowservice.CreateScheduleResponse{}
		s.service.EXPECT().CreateSchedule(gomock.Any(), gomock.Any(), gomock.Any()).Return(createResp, nil).
			Do(func(_ any, req *workflowservice.CreateScheduleRequest, _ ...any) {
				startWorkflow := req.Schedule.Action.GetStartWorkflow()
				encoding := string(startWorkflow.Memo.Fields["testMemo"].Metadata[converter.MetadataEncoding])
				if sdkFlagsAllowed[SDKFlagMemoUserDCEncode] {
					s.Equal("binary/gob", encoding)
				} else {
					s.Equal("json/plain", encoding)
				}
			})

		_, err := s.client.ScheduleClient().Create(context.Background(), options)
		s.NoError(err)
	}
	s.T().Run("old behavior", func(t *testing.T) {
		orig := sdkFlagsAllowed[SDKFlagMemoUserDCEncode]
		sdkFlagsAllowed[SDKFlagMemoUserDCEncode] = false
		defer func() { sdkFlagsAllowed[SDKFlagMemoUserDCEncode] = orig }()
		testFn()
	})
	s.T().Run("default behavior", func(t *testing.T) {
		s.True(sdkFlagsAllowed[SDKFlagMemoUserDCEncode])
		testFn()
	})
}

func (s *scheduleClientTestSuite) TestCreateScheduleWorkflowMemoUserAndDefaultConverterFail() {
	testFn := func() {
		dc := failingMemoDataConverter{
			delegate: converter.GetDefaultDataConverter(),
		}
		s.client = NewServiceClient(s.service, nil, ClientOptions{DataConverter: dc})

		memo := map[string]any{
			"testMemo": make(chan int),
		}
		wf := func(ctx Context) string { panic("this is just a stub") }

		options := ScheduleOptions{
			ID: scheduleID,
			Spec: ScheduleSpec{
				CronExpressions: []string{"*"},
			},
			Action: &ScheduleWorkflowAction{
				Workflow:                 wf,
				ID:                       workflowID,
				TaskQueue:                taskqueue,
				WorkflowExecutionTimeout: timeoutInSeconds,
				WorkflowTaskTimeout:      timeoutInSeconds,
				Memo:                     memo,
			},
		}

		s.service.EXPECT().CreateSchedule(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

		_, err := s.client.ScheduleClient().Create(context.Background(), options)
		s.Error(err)
		if sdkFlagsAllowed[SDKFlagMemoUserDCEncode] {
			s.ErrorContains(err, "failingMemoDataConverter memo encoding failed")
		} else {
			s.ErrorContains(err, "unsupported type: chan int")
		}
	}

	s.T().Run("old behavior", func(t *testing.T) {
		orig := sdkFlagsAllowed[SDKFlagMemoUserDCEncode]
		sdkFlagsAllowed[SDKFlagMemoUserDCEncode] = false
		defer func() { sdkFlagsAllowed[SDKFlagMemoUserDCEncode] = orig }()
		testFn()
	})
	s.T().Run("default behavior", func(t *testing.T) {
		s.True(sdkFlagsAllowed[SDKFlagMemoUserDCEncode])
		testFn()
	})
}

func (s *scheduleClientTestSuite) TestDescribeSchedulePopulatesPriority() {
	describeResponse := &workflowservice.DescribeScheduleResponse{
		Schedule: &schedulepb.Schedule{
			Action: &schedulepb.ScheduleAction{
				Action: &schedulepb.ScheduleAction_StartWorkflow{
					StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
						WorkflowId:   workflowID,
						WorkflowType: &commonpb.WorkflowType{Name: "wf-type"},
						TaskQueue:    &taskqueuepb.TaskQueue{Name: taskqueue},
						Priority: &commonpb.Priority{
							PriorityKey:    3,
							FairnessKey:    "fairness-key",
							FairnessWeight: 2.5,
						},
					},
				},
			},
		},
		Info: &schedulepb.ScheduleInfo{},
	}
	s.service.EXPECT().DescribeSchedule(gomock.Any(), gomock.Any(), gomock.Any()).Return(describeResponse, nil).Times(1)

	description, err := s.client.ScheduleClient().GetHandle(context.Background(), scheduleID).Describe(context.Background())
	s.NoError(err)

	action, ok := description.Schedule.Action.(*ScheduleWorkflowAction)
	s.True(ok)
	s.Equal(3, action.Priority.PriorityKey)
	s.Equal("fairness-key", action.Priority.FairnessKey)
	s.Equal(float32(2.5), action.Priority.FairnessWeight)
}
