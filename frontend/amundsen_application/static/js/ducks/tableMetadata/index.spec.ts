import { testSaga } from 'redux-saga-test-plan';

import {
  PreviewData,
  TablePreviewQueryParams,
  TableMetadata,
  TableQualityChecks,
  Tag,
  User,
} from 'interfaces';

import { dashboardSummary } from 'fixtures/metadata/dashboard';
import globalState from 'fixtures/globalState';

import * as API from './api/v0';

import reducer, {
  getTableData,
  getTableDataFailure,
  getTableDataSuccess,
  getTableDashboardsResponse,
  getTableDescription,
  getTableDescriptionFailure,
  getTableDescriptionSuccess,
  updateTableDescription,
  getColumnDescription,
  getColumnDescriptionFailure,
  getColumnDescriptionSuccess,
  updateColumnDescription,
  getPreviewData,
  getPreviewDataFailure,
  getPreviewDataSuccess,
  initialTableDataState,
  initialState,
  TableMetadataReducerState,
  getTableQualityChecks,
  getTableQualityChecksSuccess,
  getTableQualityChecksFailure,
  emptyQualityChecks,
} from './reducer';

import {
  getTableDataWatcher,
  getTableDataWorker,
  getTableDescriptionWatcher,
  getTableDescriptionWorker,
  updateTableDescriptionWatcher,
  updateTableDescriptionWorker,
  getColumnDescriptionWatcher,
  getColumnDescriptionWorker,
  updateColumnDescriptionWatcher,
  updateColumnDescriptionWorker,
  getPreviewDataWatcher,
  getPreviewDataWorker,
  getTableQualityChecksWatcher,
  getTableQualityChecksWorker,
} from './sagas';

import {
  GetTableData,
  GetTableDescription,
  UpdateTableDescription,
  GetColumnDescription,
  UpdateColumnDescription,
  GetPreviewData,
  GetTableQualityChecks,
} from './types';

describe('tableMetadata ducks', () => {
  let expectedData: TableMetadata;
  let expectedOwners: { [id: string]: User };
  let expectedTags: Tag[];
  let expectedStatus: number;
  let mockSuccess;
  let mockFailure;
  let testKey: string;
  let testIndex: string;
  let testSource: string;
  let testTableQualityChecks: TableQualityChecks;

  let columnName: string;
  let emptyPreviewData: PreviewData;
  let newDescription: string;
  let previewData: PreviewData;
  let queryParams: TablePreviewQueryParams;

  beforeAll(() => {
    expectedData = globalState.tableMetadata.tableData;
    expectedOwners = {
      testId: {
        display_name: 'test',
        profile_url: 'test.io',
        email: 'test@test.com',
        user_id: 'testId',
      },
    };
    expectedTags = [
      { tag_count: 2, tag_name: 'test' },
      { tag_count: 1, tag_name: 'test2' },
    ];
    expectedStatus = 200;

    mockSuccess = jest.fn().mockImplementation(() => {});
    mockFailure = jest.fn().mockImplementation(() => {});

    testKey = 'tableKey';
    testIndex = '3';
    testSource = 'search';
    testTableQualityChecks = {
      num_checks_total: 10,
      num_checks_failed: 2,
      num_checks_success: 8,
      external_url: 'test_url',
      last_run_timestamp: null,
    };

    columnName = 'colName';
    emptyPreviewData = {
      columns: [],
      data: [],
      error_text: 'Test text',
    };
    newDescription = 'testVal';
    previewData = {
      columns: [
        { column_name: 'col_id', column_type: 'BIGINT' },
        { column_name: 'col_1', column_type: 'VARCHAR' },
      ],
      data: [{ id: '1' }, { id: '2' }, { id: '3' }],
      error_text: 'Test text',
    };
    queryParams = {
      database: 'testDb',
      schema: 'testSchema',
      tableName: 'testName',
      cluster: 'testCluster',
    };
  });

  describe('actions', () => {
    it('getTableData - returns the action to get table metadata', () => {
      const action = getTableData(testKey, testIndex, testSource);
      const { payload } = action;
      expect(action.type).toBe(GetTableData.REQUEST);
      expect(payload.key).toBe(testKey);
      expect(payload.searchIndex).toBe(testIndex);
      expect(payload.source).toBe(testSource);
    });

    it('getTableDataFailure - returns the action to process failure', () => {
      const action = getTableDataFailure();
      const { payload } = action;
      expect(action.type).toBe(GetTableData.FAILURE);
      expect(payload.data).toBe(initialTableDataState);
      expect(payload.owners).toEqual({});
      expect(payload.statusCode).toBe(500);
      expect(payload.tags).toEqual([]);
    });

    it('getTableDataSuccess - returns the action to process success', () => {
      const action = getTableDataSuccess(
        expectedData,
        expectedOwners,
        expectedStatus,
        expectedTags
      );
      const { payload } = action;
      expect(action.type).toBe(GetTableData.SUCCESS);
      expect(payload.data).toBe(expectedData);
      expect(payload.owners).toEqual(expectedOwners);
      expect(payload.statusCode).toBe(expectedStatus);
      expect(payload.tags).toEqual(expectedTags);
    });

    it('getTableDescription - returns the action to get the table description', () => {
      const action = getTableDescription(mockSuccess, mockFailure);
      const { payload } = action;
      expect(action.type).toBe(GetTableDescription.REQUEST);
      expect(payload.onSuccess).toBe(mockSuccess);
      expect(payload.onFailure).toBe(mockFailure);
    });

    it('getTableDescriptionFailure - returns the action to process failure', () => {
      const action = getTableDescriptionFailure(expectedData);
      const { payload } = action;
      expect(action.type).toBe(GetTableDescription.FAILURE);
      expect(payload.tableMetadata).toBe(expectedData);
    });

    it('getTableDescriptionSuccess - returns the action to process success', () => {
      const action = getTableDescriptionSuccess(expectedData);
      const { payload } = action;
      expect(action.type).toBe(GetTableDescription.SUCCESS);
      expect(payload.tableMetadata).toBe(expectedData);
    });

    it('updateTableDescription - returns the action to update the table description', () => {
      const action = updateTableDescription(
        newDescription,
        mockSuccess,
        mockFailure
      );
      const { payload } = action;
      expect(action.type).toBe(UpdateTableDescription.REQUEST);
      expect(payload.newValue).toBe(newDescription);
      expect(payload.onSuccess).toBe(mockSuccess);
      expect(payload.onFailure).toBe(mockFailure);
    });

    it('getColumnDescription - returns the action to get a column description given the index', () => {
      const action = getColumnDescription(columnName, mockSuccess, mockFailure);
      const { payload } = action;
      expect(action.type).toBe(GetColumnDescription.REQUEST);
      expect(payload.columnName).toBe(columnName);
      expect(payload.onSuccess).toBe(mockSuccess);
      expect(payload.onFailure).toBe(mockFailure);
    });

    it('getColumnDescriptionFailure - returns the action to process failure', () => {
      const action = getColumnDescriptionFailure(expectedData);
      const { payload } = action;
      expect(action.type).toBe(GetColumnDescription.FAILURE);
      expect(payload.tableMetadata).toBe(expectedData);
    });

    it('getColumnDescriptionSuccess - returns the action to process success', () => {
      const action = getColumnDescriptionSuccess(expectedData);
      const { payload } = action;
      expect(action.type).toBe(GetColumnDescription.SUCCESS);
      expect(payload.tableMetadata).toBe(expectedData);
    });

    it('updateColumnDescription - returns the action to update the table description', () => {
      const action = updateColumnDescription(
        newDescription,
        columnName,
        mockSuccess,
        mockFailure
      );
      const { payload } = action;
      expect(action.type).toBe(UpdateColumnDescription.REQUEST);
      expect(payload.newValue).toBe(newDescription);
      expect(payload.columnName).toBe(columnName);
      expect(payload.onSuccess).toBe(mockSuccess);
      expect(payload.onFailure).toBe(mockFailure);
    });

    it('getPreviewData - returns the action to get the preview table data', () => {
      const action = getPreviewData(queryParams);
      const { payload } = action;
      expect(action.type).toBe(GetPreviewData.REQUEST);
      expect(payload.queryParams).toBe(queryParams);
    });

    it('getPreviewDataFailure - returns the action to process failure', () => {
      const status = 500;
      const action = getPreviewDataFailure(emptyPreviewData, status);
      const { payload } = action;
      expect(action.type).toBe(GetPreviewData.FAILURE);
      expect(payload.data).toBe(emptyPreviewData);
      expect(payload.status).toBe(status);
    });

    it('getPreviewDataSuccess - returns the action to process success', () => {
      const status = 200;
      const action = getPreviewDataSuccess(previewData, status);
      const { payload } = action;
      expect(action.type).toBe(GetPreviewData.SUCCESS);
      expect(payload.data).toBe(previewData);
      expect(payload.status).toBe(status);
    });

    it('getTableQualityChecks - returns the action to process failure', () => {
      const status = 500;
      const action = getTableQualityChecksFailure(status);
      const { payload } = action;
      expect(action.type).toBe(GetTableQualityChecks.FAILURE);
      expect(payload.checks).toBe(emptyQualityChecks);
      expect(payload.status).toBe(status);
    });

    it('getTableQualityChecks - returns the action to process success', () => {
      const status = 500;
      const action = getTableQualityChecksSuccess(
        testTableQualityChecks,
        status
      );
      const { payload } = action;
      expect(action.type).toBe(GetTableQualityChecks.SUCCESS);
      expect(payload.checks).toBe(testTableQualityChecks);
      expect(payload.status).toBe(status);
    });
  });

  /* TODO: Code involving nested reducers is not covered, will need more investigation */
  describe('reducer', () => {
    let testState: TableMetadataReducerState;
    beforeAll(() => {
      testState = initialState;
    });
    it('should return the existing state if action is not handled', () => {
      expect(reducer(testState, { type: 'INVALID.ACTION' })).toEqual(testState);
    });

    it('should handle GetTableDashboards.RESPONSE', () => {
      const mockDashboards = [dashboardSummary];
      const mockMessage = 'test';
      expect(
        reducer(
          testState,
          getTableDashboardsResponse(mockDashboards, mockMessage)
        )
      ).toEqual({
        ...testState,
        dashboards: {
          isLoading: false,
          dashboards: mockDashboards,
          errorMessage: mockMessage,
        },
      });
    });

    it('should handle GetTableDescription.FAILURE', () => {
      expect(
        reducer(testState, getTableDescriptionFailure(expectedData))
      ).toEqual({
        ...testState,
        tableData: expectedData,
      });
    });

    it('should handle GetTableDescription.SUCCESS', () => {
      expect(
        reducer(testState, getTableDescriptionSuccess(expectedData))
      ).toEqual({
        ...testState,
        tableData: expectedData,
      });
    });

    it('should handle GetColumnDescription.FAILURE', () => {
      expect(
        reducer(testState, getColumnDescriptionFailure(expectedData))
      ).toEqual({
        ...testState,
        tableData: expectedData,
      });
    });

    it('should handle GetColumnDescription.SUCCESS', () => {
      expect(
        reducer(testState, getColumnDescriptionSuccess(expectedData))
      ).toEqual({
        ...testState,
        tableData: expectedData,
      });
    });

    it('should handle GetPreviewData.FAILURE', () => {
      const action = getPreviewDataFailure({}, 500);
      expect(reducer(testState, action)).toEqual({
        ...testState,
        preview: action.payload,
      });
    });

    it('should handle GetPreviewData.SUCCESS', () => {
      const action = getPreviewDataSuccess(previewData, 200);
      expect(reducer(testState, action)).toEqual({
        ...testState,
        preview: action.payload,
      });
    });
  });

  describe('sagas', () => {
    describe('getTableDataWatcher', () => {
      it('takes every GetTableData.REQUEST with getTableDataWorker', () => {
        testSaga(getTableDataWatcher)
          .next()
          .takeEvery(GetTableData.REQUEST, getTableDataWorker)
          .next()
          .isDone();
      });
    });

    describe('getTableDataWorker', () => {
      it('executes flow for getting table data', () => {
        const mockResult = {
          data: expectedData,
          owners: expectedOwners,
          statusCode: expectedStatus,
          tags: expectedTags,
        };
        const mockDashboardsResult = {
          dashboards: [dashboardSummary],
        };
        testSaga(
          getTableDataWorker,
          getTableData(testKey, testIndex, testSource)
        )
          .next()
          .call(API.getTableData, testKey, testIndex, testSource)
          .next(mockResult)
          .put(
            getTableDataSuccess(
              expectedData,
              expectedOwners,
              expectedStatus,
              expectedTags
            )
          )
          .next()
          .call(API.getTableDashboards, testKey)
          .next(mockDashboardsResult)
          .put(getTableDashboardsResponse(mockDashboardsResult.dashboards))
          .next()
          .isDone();
      });

      it('handles request error on getTableData', () => {
        testSaga(getTableDataWorker, getTableData(testKey))
          .next()
          .throw(new Error())
          .put(getTableDataFailure())
          .next()
          .isDone();
      });
    });

    describe('getTableDescriptionWatcher', () => {
      it('takes every GetTableDescription.REQUEST with getTableDescriptionWorker', () => {
        testSaga(getTableDescriptionWatcher)
          .next()
          .takeEvery(GetTableDescription.REQUEST, getTableDescriptionWorker)
          .next()
          .isDone();
      });
    });

    describe('getTableDescriptionWorker', () => {
      describe('executes flow for getting table description', () => {
        let sagaTest;
        beforeAll(() => {
          const mockNewTableData: TableMetadata = initialTableDataState;
          sagaTest = (action) =>
            testSaga(getTableDescriptionWorker, action)
              .next()
              .select()
              .next(globalState)
              .call(
                API.getTableDescription,
                globalState.tableMetadata.tableData
              )
              .next(mockNewTableData)
              .put(getTableDescriptionSuccess(mockNewTableData));
        });
        it('without success callback', () => {
          sagaTest(getTableDescription()).next().isDone();
        });

        it('with success callback', () => {
          sagaTest(getTableDescription(mockSuccess, mockFailure))
            .next()
            .call(mockSuccess)
            .next()
            .isDone();
        });
      });

      describe('handles request error', () => {
        let sagaTest;
        beforeAll(() => {
          sagaTest = (action) =>
            testSaga(getTableDescriptionWorker, action)
              .next()
              .select()
              .next(globalState)
              .throw(new Error())
              .put(
                getTableDescriptionFailure(globalState.tableMetadata.tableData)
              );
        });
        it('without failure callback', () => {
          sagaTest(getTableDescription()).next().isDone();
        });

        it('with failure callback', () => {
          sagaTest(getTableDescription(mockSuccess, mockFailure))
            .next()
            .call(mockFailure)
            .next()
            .isDone();
        });
      });
    });

    describe('updateTableDescriptionWatcher', () => {
      it('takes every UpdateTableDescription.REQUEST with updateTableDescriptionWorker', () => {
        testSaga(updateTableDescriptionWatcher)
          .next()
          .takeEvery(
            UpdateTableDescription.REQUEST,
            updateTableDescriptionWorker
          )
          .next()
          .isDone();
      });
    });

    describe('updateTableDescriptionWorker', () => {
      describe('executes flow for updating table description', () => {
        let sagaTest;
        beforeAll(() => {
          sagaTest = (mockSuccess) =>
            testSaga(
              updateTableDescriptionWorker,
              updateTableDescription(newDescription, mockSuccess, undefined)
            )
              .next()
              .select()
              .next(globalState)
              .call(
                API.updateTableDescription,
                newDescription,
                globalState.tableMetadata.tableData
              );
        });
        it('without success callback', () => {
          sagaTest().next().isDone();
        });

        it('with success callback', () => {
          sagaTest(mockSuccess).next().call(mockSuccess).next().isDone();
        });
      });

      describe('handles request error', () => {
        let sagaTest;
        beforeAll(() => {
          sagaTest = (mockFailure) =>
            testSaga(
              updateTableDescriptionWorker,
              updateTableDescription(newDescription, undefined, mockFailure)
            )
              .next()
              .select()
              .next(globalState)
              .throw(new Error());
        });
        it('without failure callback', () => {
          sagaTest().next().isDone();
        });

        it('with failure callback', () => {
          sagaTest(mockFailure).call(mockFailure).next().isDone();
        });
      });
    });

    describe('getColumnDescriptionWatcher', () => {
      it('takes every GetColumnDescription.REQUEST with getColumnDescriptionWorker', () => {
        testSaga(getColumnDescriptionWatcher)
          .next()
          .takeEvery(GetColumnDescription.REQUEST, getColumnDescriptionWorker)
          .next()
          .isDone();
      });
    });

    describe('getColumnDescriptionWorker', () => {
      describe('executes flow for getting a table column description', () => {
        let sagaTest;
        beforeAll(() => {
          const mockNewTableData: TableMetadata = initialTableDataState;

          sagaTest = (action) =>
            testSaga(getColumnDescriptionWorker, action)
              .next()
              .select()
              .next(globalState)
              .call(
                API.getColumnDescription,
                action.payload.columnName,
                globalState.tableMetadata.tableData
              )
              .next(mockNewTableData)
              .put(getColumnDescriptionSuccess(mockNewTableData));
        });
        it('without success callback', () => {
          sagaTest(getColumnDescription(columnName)).next().isDone();
        });

        it('with success callback', () => {
          sagaTest(getColumnDescription(columnName, mockSuccess, mockFailure))
            .next()
            .call(mockSuccess)
            .next()
            .isDone();
        });
      });

      describe('handles request error', () => {
        let sagaTest;
        beforeAll(() => {
          sagaTest = (action) =>
            testSaga(getColumnDescriptionWorker, action)
              .next()
              .select()
              .next(globalState)
              .throw(new Error())
              .put(
                getColumnDescriptionFailure(globalState.tableMetadata.tableData)
              );
        });
        it('without failure callback', () => {
          sagaTest(getColumnDescription(columnName)).next().isDone();
        });

        it('with failure callback', () => {
          sagaTest(getColumnDescription(columnName, mockSuccess, mockFailure))
            .next()
            .call(mockFailure)
            .next()
            .isDone();
        });
      });
    });

    describe('updateColumnDescriptionWatcher', () => {
      it('takes every UpdateColumnDescription.REQUEST with updateColumnDescriptionWorker', () => {
        testSaga(updateColumnDescriptionWatcher)
          .next()
          .takeEvery(
            UpdateColumnDescription.REQUEST,
            updateColumnDescriptionWorker
          )
          .next()
          .isDone();
      });
    });

    describe('updateColumnDescriptionWorker', () => {
      describe('executes flow for updating a table column description', () => {
        let sagaTest;
        beforeAll(() => {
          sagaTest = (mockSuccess) =>
            testSaga(
              updateColumnDescriptionWorker,
              updateColumnDescription(
                newDescription,
                columnName,
                mockSuccess,
                undefined
              )
            )
              .next()
              .select()
              .next(globalState)
              .call(
                API.updateColumnDescription,
                newDescription,
                columnName,
                globalState.tableMetadata.tableData
              );
        });
        it('without success callback', () => {
          sagaTest().next().isDone();
        });

        it('with success callback', () => {
          sagaTest(mockSuccess).next().call(mockSuccess).next().isDone();
        });
      });

      describe('handles request error', () => {
        let sagaTest;
        beforeAll(() => {
          sagaTest = (mockFailure) =>
            testSaga(
              updateColumnDescriptionWorker,
              updateColumnDescription(
                newDescription,
                columnName,
                undefined,
                mockFailure
              )
            )
              .next()
              .select()
              .next(globalState)
              .throw(new Error());
        });
        it('without failure callback', () => {
          sagaTest().next().isDone();
        });

        it('with failure callback', () => {
          sagaTest(mockFailure).call(mockFailure).next().isDone();
        });
      });
    });

    describe('getPreviewDataWatcher', () => {
      it('takes every GetPreviewData.REQUEST with getPreviewDataWorker', () => {
        testSaga(getPreviewDataWatcher)
          .next()
          .takeLatest(GetPreviewData.REQUEST, getPreviewDataWorker)
          .next()
          .isDone();
      });
    });

    describe('getPreviewDataWorker', () => {
      it('executes flow for getting preview data', () => {
        testSaga(getPreviewDataWorker, getPreviewData(queryParams))
          .next()
          .call(API.getPreviewData, queryParams)
          .next({ data: previewData, status: 200 })
          .put(getPreviewDataSuccess(previewData, 200))
          .next()
          .isDone();
      });

      it('handles request error', () => {
        testSaga(getPreviewDataWorker, getPreviewData(queryParams))
          .next()
          .call(API.getPreviewData, queryParams)
          // @ts-ignore TODO: Investigate why redux-saga-test-plan throw() complains
          .throw({ data: previewData, status: 500 })
          .put(getPreviewDataFailure(previewData, 500))
          .next()
          .isDone();
      });
    });

    describe('getTableQualityChecksWatcher', () => {
      it('takes every GetTableQualityChecks.REQUEST with GetTableQualityChecksWorker', () => {
        testSaga(getTableQualityChecksWatcher)
          .next()
          .takeLatest(
            GetTableQualityChecks.REQUEST,
            getTableQualityChecksWorker
          )
          .next()
          .isDone();
      });
    });

    describe('getTableQualityChecksWorker', () => {
      it('executes flow for getting table quality checks', () => {
        testSaga(getTableQualityChecksWorker, getTableQualityChecks(testKey))
          .next()
          .call(API.getTableQualityChecksSummary, testKey)
          .next({ checks: testTableQualityChecks, status: 200 })
          .put(getTableQualityChecksSuccess(testTableQualityChecks, 200))
          .next()
          .isDone();
      });

      it('handles request error', () => {
        testSaga(getTableQualityChecksWorker, getTableQualityChecks(testKey))
          .next()
          .call(API.getTableQualityChecksSummary, testKey)
          // @ts-ignore
          .throw({ status: 500 })
          .put(getTableQualityChecksFailure(500))
          .next()
          .isDone();
      });
    });
  });
});
