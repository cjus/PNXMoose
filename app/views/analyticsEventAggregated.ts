import typia from "typia";
import { MaterializedView, sql } from "@514labs/moose-lib";
import { AnalyticsEventPipeline } from "../ingest/models";

interface AnalyticsEventAggregated {
  eventDate: Date;
  appName: string;
  eventType: string;
  stage: string;
  // session granularity metrics - using aggregate function types
  uniqueSessionsState: string; // AggregateFunction(uniq, String)
  uniqueVisitorsState: string; // AggregateFunction(uniq, String)
  uniquePageviewsState: string; // AggregateFunction(uniq, String)
  totalEventsState: string; // AggregateFunction(count, *)
  uniqueUsersState: string; // AggregateFunction(uniq, String)
  uniqueIPsState: string; // AggregateFunction(uniq, String)
  chromeUsersState: string; // AggregateFunction(countIf, UInt8)
  safariUsersState: string; // AggregateFunction(countIf, UInt8)
  firefoxUsersState: string; // AggregateFunction(countIf, UInt8)
  macOSUsersState: string; // AggregateFunction(countIf, UInt8)
  windowsUsersState: string; // AggregateFunction(countIf, UInt8)
  navigationEventsState: string; // AggregateFunction(countIf, UInt8)
}

const analyticsTable = AnalyticsEventPipeline.table!;
// const analyticsColumns = analyticsTable.columns;

export const AnalyticsEventAggregatedMV =
  new MaterializedView<AnalyticsEventAggregated>({
    tableName: "AnalyticsEventAggregated",
    materializedViewName: "AnalyticsEventAggregated_MV",
    orderByFields: ["eventDate", "appName", "eventType"],
    selectStatement: sql`SELECT
    toDate(timestamp) as eventDate,
    appName,
    eventType,
    stage,
    uniqState(sessionId) as uniqueSessionsState,
    uniqState(visitorId) as uniqueVisitorsState,
    uniqState(pageviewId) as uniquePageviewsState,
    countState(*) as totalEventsState,
    uniqState(userId) as uniqueUsersState,
    uniqState(sourceIp) as uniqueIPsState,
    countIfState(browserName = 'Chrome') as chromeUsersState,
    countIfState(browserName = 'Safari') as safariUsersState,
    countIfState(browserName = 'Firefox') as firefoxUsersState,
    countIfState(operatingSystem = 'macOS') as macOSUsersState,
    countIfState(operatingSystem = 'Windows') as windowsUsersState,
    countIfState(navigationHref IS NOT NULL) as navigationEventsState
  FROM AnalyticsEvent
  GROUP BY toDate(timestamp), appName, eventType, stage
  `,
    selectTables: [analyticsTable],
  });
