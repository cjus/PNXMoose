import { Api, MooseCache, joinQueries } from "@514labs/moose-lib";
import { tags } from "typia";

interface UserFunnelQueryParams {
  funnelType: "video-player" | "signup" | "video-view" | "custom";
  startDate?: string; // YYYY-MM-DD format
  endDate?: string; // YYYY-MM-DD format
  appName?: string;
  stage?: "production" | "staging" | "development";
}

interface FunnelStep {
  stepName: string;
  reached: number;
  reachedRate: number;
  conversionRate: number;
  avgTimeToNextStepSec: number;
  dropoffCount: number;
}

interface DropoffPoint {
  fromStep: string;
  toStep: string;
  dropoffRate: number;
  dropoffCount: number;
}

interface UserFunnelResponse {
  funnelType: string;
  totalSessions: number;
  steps: FunnelStep[];
  dropoffPoints: DropoffPoint[];
}

export const UserFunnelApi = new Api<
  UserFunnelQueryParams,
  UserFunnelResponse
>(
  "user-funnel",
  async ({ funnelType, startDate, endDate, appName, stage }, { client, sql }) => {
    const cache = await MooseCache.get();
    const cacheKey = `user-funnel:${funnelType}:${appName || "all"}:${
      stage || "all"
    }:${startDate || ""}:${endDate || ""}`;

    const cached = await cache.get<UserFunnelResponse>(cacheKey);
    if (cached) {
      return cached;
    }

    // Build filters
    const filters = [sql`1`];
    if (appName) filters.push(sql`appName = ${appName}`);
    if (stage) filters.push(sql`stage = ${stage}`);
    if (startDate) filters.push(sql`toDate(timestamp) >= ${startDate}`);
    if (endDate) filters.push(sql`toDate(timestamp) <= ${endDate}`);

    const where =
      filters.length > 0
        ? joinQueries({
            values: filters,
            separator: " AND ",
            prefix: "WHERE ",
          })
        : sql``;

    // Define funnel steps based on funnelType
    let stepDefinitions: { name: string; condition: string }[] = [];

    if (funnelType === "video-player") {
      stepDefinitions = [
        { name: "page_view", condition: "eventType = 'page_view'" },
        {
          name: "player_loaded",
          condition:
            "eventType = 'player_loaded' OR eventType LIKE '%player%load%'",
        },
        {
          name: "video_start",
          condition:
            "eventType = 'video_start' OR eventType LIKE '%video%start%'",
        },
        {
          name: "play",
          condition:
            "eventType LIKE '%play%' AND eventType NOT LIKE '%pause%'",
        },
      ];
    } else if (funnelType === "signup") {
      stepDefinitions = [
        { name: "page_view", condition: "eventType = 'page_view'" },
        {
          name: "signup_form_viewed",
          condition: "eventType LIKE '%signup%form%' OR navigationHref LIKE '%signup%'",
        },
        { name: "form_submitted", condition: "eventType LIKE '%submit%'" },
        {
          name: "signup_completed",
          condition: "eventType LIKE '%signup%complete%'",
        },
      ];
    } else if (funnelType === "video-view") {
      stepDefinitions = [
        { name: "page_view", condition: "eventType = 'page_view'" },
        { name: "video_page", condition: "navigationHref LIKE '%video%'" },
        { name: "player_loaded", condition: "eventType LIKE '%player%load%'" },
        { name: "video_start", condition: "eventType LIKE '%video%start%'" },
      ];
    }

    // Build maxIf conditions for each step dynamically
    // Since we can't use sql.raw, we'll build static queries for each funnel type
    let query;
    
    if (funnelType === "video-player") {
      query = sql`
        WITH FunnelSteps AS (
          SELECT
            sessionId,
            maxIf(timestamp, eventType = 'page_view') as page_viewTime,
            maxIf(timestamp, eventType = 'player_loaded' OR eventType LIKE '%player%load%') as player_loadedTime,
            maxIf(timestamp, eventType = 'video_start' OR eventType LIKE '%video%start%') as video_startTime,
            maxIf(timestamp, eventType LIKE '%play%' AND eventType NOT LIKE '%pause%') as playTime
          FROM AnalyticsEvent
          ${where}
          GROUP BY sessionId
        ),
        StepCounts AS (
          SELECT
            count(*) as totalSessions,
            countIf(page_viewTime > toDateTime('1970-01-01')) as page_viewReached,
            countIf(player_loadedTime > toDateTime('1970-01-01')) as player_loadedReached,
            countIf(video_startTime > toDateTime('1970-01-01')) as video_startReached,
            countIf(playTime > toDateTime('1970-01-01')) as playReached
          FROM FunnelSteps
        )
        SELECT
          toInt32(totalSessions) as totalSessions,
          toInt32(page_viewReached) as page_viewReached,
          toInt32(player_loadedReached) as player_loadedReached,
          toInt32(video_startReached) as video_startReached,
          toInt32(playReached) as playReached
        FROM StepCounts
      `;
    } else if (funnelType === "signup") {
      query = sql`
        WITH FunnelSteps AS (
          SELECT
            sessionId,
            maxIf(timestamp, eventType = 'page_view') as page_viewTime,
            maxIf(timestamp, eventType LIKE '%signup%form%' OR navigationHref LIKE '%signup%') as signup_form_viewedTime,
            maxIf(timestamp, eventType LIKE '%submit%') as form_submittedTime,
            maxIf(timestamp, eventType LIKE '%signup%complete%') as signup_completedTime
          FROM AnalyticsEvent
          ${where}
          GROUP BY sessionId
        ),
        StepCounts AS (
          SELECT
            count(*) as totalSessions,
            countIf(page_viewTime > toDateTime('1970-01-01')) as page_viewReached,
            countIf(signup_form_viewedTime > toDateTime('1970-01-01')) as signup_form_viewedReached,
            countIf(form_submittedTime > toDateTime('1970-01-01')) as form_submittedReached,
            countIf(signup_completedTime > toDateTime('1970-01-01')) as signup_completedReached
          FROM FunnelSteps
        )
        SELECT
          toInt32(totalSessions) as totalSessions,
          toInt32(page_viewReached) as page_viewReached,
          toInt32(signup_form_viewedReached) as signup_form_viewedReached,
          toInt32(form_submittedReached) as form_submittedReached,
          toInt32(signup_completedReached) as signup_completedReached
        FROM StepCounts
      `;
    } else if (funnelType === "video-view") {
      query = sql`
        WITH FunnelSteps AS (
          SELECT
            sessionId,
            maxIf(timestamp, eventType = 'page_view') as page_viewTime,
            maxIf(timestamp, navigationHref LIKE '%video%') as video_pageTime,
            maxIf(timestamp, eventType LIKE '%player%load%') as player_loadedTime,
            maxIf(timestamp, eventType LIKE '%video%start%') as video_startTime
          FROM AnalyticsEvent
          ${where}
          GROUP BY sessionId
        ),
        StepCounts AS (
          SELECT
            count(*) as totalSessions,
            countIf(page_viewTime > toDateTime('1970-01-01')) as page_viewReached,
            countIf(video_pageTime > toDateTime('1970-01-01')) as video_pageReached,
            countIf(player_loadedTime > toDateTime('1970-01-01')) as player_loadedReached,
            countIf(video_startTime > toDateTime('1970-01-01')) as video_startReached
          FROM FunnelSteps
        )
        SELECT
          toInt32(totalSessions) as totalSessions,
          toInt32(page_viewReached) as page_viewReached,
          toInt32(video_pageReached) as video_pageReached,
          toInt32(player_loadedReached) as player_loadedReached,
          toInt32(video_startReached) as video_startReached
        FROM StepCounts
      `;
    } else {
      // For custom funnel type, return empty response
      return {
        funnelType,
        totalSessions: 0,
        steps: [],
        dropoffPoints: [],
      };
    }

    try {
      const data = await client.query.execute<any>(query);
      const result: any[] = await data.json();

      if (result.length === 0) {
        return {
          funnelType,
          totalSessions: 0,
          steps: [],
          dropoffPoints: [],
        };
      }

      const row = result[0];
      const totalSessions = parseInt(row.totalSessions.toString()) || 0;

      // Build steps array using the step definitions
      const steps: FunnelStep[] = stepDefinitions.map((stepDef, idx) => {
        const fieldName = `${stepDef.name}Reached`;
        const reached = parseInt(
          (row as any)[fieldName]?.toString() || "0"
        );
        
        let previousReached: number;
        if (idx === 0) {
          previousReached = totalSessions;
        } else {
          const previousFieldName = `${stepDefinitions[idx - 1].name}Reached`;
          previousReached = parseInt(
            (row as any)[previousFieldName]?.toString() || "0"
          );
        }

        const reachedRate = totalSessions > 0 ? (reached / totalSessions) * 100 : 0;
        const conversionRate =
          previousReached > 0 ? (reached / previousReached) * 100 : 0;
        const dropoffCount = previousReached - reached;

        return {
          stepName: stepDef.name,
          reached,
          reachedRate: Math.round(reachedRate * 100) / 100,
          conversionRate: Math.round(conversionRate * 100) / 100,
          avgTimeToNextStepSec: 0, // Would require additional query with timestamps
          dropoffCount,
        };
      });

      // Build dropoff points
      const dropoffPoints: DropoffPoint[] = [];
      for (let i = 0; i < steps.length - 1; i++) {
        const fromStep = steps[i];
        const toStep = steps[i + 1];
        const dropoffRate =
          fromStep.reached > 0
            ? ((fromStep.reached - toStep.reached) / fromStep.reached) * 100
            : 0;

        dropoffPoints.push({
          fromStep: fromStep.stepName,
          toStep: toStep.stepName,
          dropoffRate: Math.round(dropoffRate * 100) / 100,
          dropoffCount: fromStep.reached - toStep.reached,
        });
      }

      const response: UserFunnelResponse = {
        funnelType,
        totalSessions,
        steps,
        dropoffPoints,
      };

      // Cache for 15 minutes
      await cache.set(cacheKey, response, 900);

      return response;
    } catch (error) {
      console.error("User Funnel API error:", error);
      return {
        funnelType,
        totalSessions: 0,
        steps: [],
        dropoffPoints: [],
      };
    }
  }
);

