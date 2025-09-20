import typia from "typia";
import { MaterializedView, sql } from "@514labs/moose-lib";
import { HLSEventPipeline } from "../ingest/models";

interface HLSEventAggregated {
  eventDate: Date;
  appName: string;
  eventType: string;
  stage: string;
  uniqueSessionsState: number & typia.tags.Type<"uint64">;
  uniqueVisitorsState: number & typia.tags.Type<"uint64">;
  uniquePageviewsState: number & typia.tags.Type<"uint64">;
  totalEventsState: number & typia.tags.Type<"uint64">;
  uniqueUsersState: number & typia.tags.Type<"uint64">;
  avgBitrateState: number & typia.tags.Type<"float">;
  maxBitrateState: number & typia.tags.Type<"uint64">;
  minBitrateState: number & typia.tags.Type<"uint64">;
  levelSwitchesState: number & typia.tags.Type<"uint64">;
  playbackStartsState: number & typia.tags.Type<"uint64">;
  avgFragmentDurationState: number & typia.tags.Type<"float">;
}

const hlsTable = HLSEventPipeline.table!;
// const hlsColumns = hlsTable.columns;

export const HLSEventAggregatedMV = new MaterializedView<HLSEventAggregated>({
  tableName: "HLSEventAggregated",
  materializedViewName: "HLSEventAggregated_MV",
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
    avgState(bitrate) as avgBitrateState,
    maxState(bitrate) as maxBitrateState,
    minState(bitrate) as minBitrateState,
    countIfState(eventType = 'level-switched') as levelSwitchesState,
    countIfState(eventType = 'playback-started') as playbackStartsState,
    avgState(fragmentDuration) as avgFragmentDurationState
  FROM HLSEvent
  GROUP BY toDate(timestamp), appName, eventType, stage
  `,
  selectTables: [hlsTable],
});
