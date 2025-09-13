import typia from "typia";
import { MaterializedView, sql } from "@514labs/moose-lib";
import { AnalyticsEventPipeline } from "../ingest/models";

interface AnalyticsEventAggregated {
  eventDate: Date;
  appName: string;
  eventType: string;
  stage: string;
  totalEventsState: number & typia.tags.Type<"int64">;
  uniqueUsersState: number & typia.tags.Type<"int64">;
  uniqueIPsState: number & typia.tags.Type<"int64">;
  chromeUsersState: number & typia.tags.Type<"int64">;
  safariUsersState: number & typia.tags.Type<"int64">;
  firefoxUsersState: number & typia.tags.Type<"int64">;
  macOSUsersState: number & typia.tags.Type<"int64">;
  windowsUsersState: number & typia.tags.Type<"int64">;
  navigationEventsState: number & typia.tags.Type<"int64">;
}

const analyticsTable = AnalyticsEventPipeline.table!;
const analyticsColumns = analyticsTable.columns;

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
