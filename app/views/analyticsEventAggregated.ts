import typia from "typia";
import { MaterializedView, sql } from "@514labs/moose-lib";
import { AnalyticsEventPipeline } from "../ingest/models";

interface AnalyticsEventAggregated {
  eventDate: Date;
  appName: string;
  eventType: string;
  stage: string;
  // session granularity metrics
  uniqueSessionsState: number & typia.tags.Type<"uint64">;
  uniqueVisitorsState: number & typia.tags.Type<"uint64">;
  uniquePageviewsState: number & typia.tags.Type<"uint64">;
  totalEventsState: number & typia.tags.Type<"uint64">;
  uniqueUsersState: number & typia.tags.Type<"uint64">;
  uniqueIPsState: number & typia.tags.Type<"uint64">;
  chromeUsersState: number & typia.tags.Type<"uint64">;
  safariUsersState: number & typia.tags.Type<"uint64">;
  firefoxUsersState: number & typia.tags.Type<"uint64">;
  macOSUsersState: number & typia.tags.Type<"uint64">;
  windowsUsersState: number & typia.tags.Type<"uint64">;
  navigationEventsState: number & typia.tags.Type<"uint64">;
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
