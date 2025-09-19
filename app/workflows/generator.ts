import { Task, Workflow } from "@514labs/moose-lib";
import { PNXEvent } from "../ingest/models";
import { faker } from "@faker-js/faker";
import { randomUUID } from "crypto";

// Data model for OLAP Table (commented out for now)
// interface PNXWorkflow {
//   id: Key<string>;
//   success: boolean;
//   message: string;
// }

// Create OLAP Table - temporarily commented out to isolate build issue
// const workflowTable = new OlapTable<PNXWorkflow>("PNXWorkflow", {
//   orderByFields: ["id"],
// });

export const ingest = new Task<null, void>("ingest", {
  run: async () => {
    for (let i = 0; i < 1000; i++) {
      const eventType = faker.helpers.arrayElement([
        "analytics",
        "hls",
        "navigation",
        "authentication",
        "metric",
        "error",
      ]);

      const pnxEvent: PNXEvent = {
        eventId: randomUUID(),
        appName: faker.helpers.arrayElement(["pnxplayer", "PNXStudios-Website"]),
        appVersion: faker.system.semver(),
        userId: faker.internet.email(),
        type: eventType,
        event:
          eventType === "analytics"
            ? faker.helpers.arrayElement(["panel-close", "play", "pause", "mark-start"])
            : eventType === "hls"
              ? faker.helpers.arrayElement(["level-switched", "playback-started", "buffer-stalled"])
              : eventType === "navigation"
                ? faker.helpers.arrayElement(["nav_link_clicked", "tracked_link_clicked"])
                : eventType === "authentication"
                  ? faker.helpers.arrayElement(["login_success", "logout", "signup_success"])
                  : eventType === "metric"
                    ? faker.helpers.arrayElement(["page_load_time", "api_response_time"])
                    : "javascript_error",

        // Common optional fields
        videoId: Math.random() < 0.4 ? faker.string.uuid() : undefined,

        // Analytics-specific fields
        source: eventType === "analytics" && Math.random() < 0.5 ? "panel-manager" : undefined,
        href:
          (eventType === "analytics" || eventType === "navigation") && Math.random() < 0.3
            ? faker.internet.url()
            : undefined,

        // HLS-specific fields
        level:
          eventType === "hls" && Math.random() < 0.8
            ? faker.number.int({ min: 0, max: 5 })
            : undefined,
        videoIndex:
          eventType === "hls" && Math.random() < 0.8
            ? faker.number.int({ min: 0, max: 3 })
            : undefined,
        bitrate:
          eventType === "hls" && Math.random() < 0.8
            ? faker.number.int({ min: 500000, max: 8000000 })
            : undefined,
        resolution:
          eventType === "hls" && Math.random() < 0.8
            ? faker.helpers.arrayElement(["854x480", "1280x720", "1920x1080"])
            : undefined,
        bandwidth:
          eventType === "hls" && Math.random() < 0.8
            ? faker.number.int({ min: 500000, max: 8000000 })
            : undefined,
        fragmentDuration:
          eventType === "hls" && Math.random() < 0.6
            ? faker.number.float({ min: 2, max: 10 })
            : undefined,

        // Navigation-specific fields
        action:
          eventType === "navigation"
            ? faker.helpers.arrayElement(["nav_link_clicked", "tracked_link_clicked"])
            : undefined,
        eventName:
          eventType === "navigation" && Math.random() < 0.5
            ? faker.helpers.arrayElement(["cta_click", "menu_click"])
            : undefined,

        // Authentication-specific fields
        method:
          eventType === "authentication"
            ? faker.helpers.arrayElement(["email_password", "google_oauth"])
            : undefined,

        // Error-specific fields
        message:
          eventType === "error"
            ? faker.helpers.arrayElement([
                "Cannot read property 'play' of null",
                "Network request failed",
                "Timeout error",
              ])
            : undefined,
        stack: eventType === "error" && Math.random() < 0.7 ? faker.lorem.sentences(3) : undefined,

        // Request metadata
        requestTimeEpoch: Date.now(),
        domainName: "api.pnxstudios.com",
        stage: "development",
        sourceIp: faker.internet.ip(),
        userAgent: faker.internet.userAgent(),
      };

      try {
        const response = await fetch("http://localhost:4000/ingest/PNXEvent", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify(pnxEvent),
        });

        if (!response.ok) {
          console.log(`Failed to ingest record ${i}: ${response.status} ${response.statusText}`);
          // Insert ingestion result into OLAP table
          // workflowTable.insert([
          //   { id: "1", success: false, message: response.statusText },
          // ]);
        }
      } catch (error) {
        console.log(`Error ingesting record ${i}: ${error}`);
        // workflowTable.insert([
        //   { id: "1", success: false, message: error.message },
        // ]);
      }

      // Add a small delay to avoid overwhelming the server
      if (i % 100 === 0) {
        console.log(`Ingested ${i} records...`);
        // workflowTable.insert([
        //   { id: "1", success: true, message: `Ingested ${i} records` },
        // ]);
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    }
  },
  retries: 3,
  timeout: "30s",
});

export const workflow = new Workflow("generator", {
  startingTask: ingest,
  retries: 3,
  timeout: "30s",
  // schedule: "@every 5s",
});
