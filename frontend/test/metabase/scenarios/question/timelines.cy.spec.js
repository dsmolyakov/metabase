import { restore, visitQuestion, sidebar } from "__support__/e2e/cypress";

describe("scenarios > collections > timelines", () => {
  beforeEach(() => {
    restore();
  });

  describe("as admin", () => {
    beforeEach(() => {
      cy.signInAsAdmin();
      cy.intercept("GET", "/api/collection/root").as("getCollection");
      cy.intercept("POST", "/api/timeline-event").as("createEvent");
      cy.intercept("PUT", "/api/timeline-event/**").as("updateEvent");
    });

    it("should create the first event and timeline", () => {
      visitQuestion(3);
      cy.wait("@getCollection");
      cy.findByTextEnsureVisible("Visualization");

      cy.findByLabelText("calendar icon").click();
      cy.findByTextEnsureVisible("Add an event").click();

      cy.findByLabelText("Event name").type("RC1");
      cy.findByLabelText("Date").type("10/20/2018");
      cy.button("Create").click();
      cy.wait("@createEvent");

      cy.findByTextEnsureVisible("Our analytics events");
      cy.findByText("RC1");
    });

    it("should create an event within the default timeline", () => {
      cy.createTimelineWithEvents({
        timeline: { name: "Releases" },
        events: [{ name: "RC1", timestamp: "2018-10-20T00:00:00Z" }],
      });

      visitQuestion(3);
      cy.wait("@getCollection");
      cy.findByTextEnsureVisible("Visualization");

      cy.findByLabelText("calendar icon").click();
      cy.findByTextEnsureVisible("Add an event").click();

      cy.findByLabelText("Event name").type("RC2");
      cy.findByLabelText("Date").type("10/30/2018");
      cy.button("Create").click();
      cy.wait("@createEvent");

      cy.findByTextEnsureVisible("Releases");
      cy.findByText("RC1");
      cy.findByText("RC2");
    });

    it("should edit an event", () => {
      cy.createTimelineWithEvents({
        timeline: { name: "Releases" },
        events: [{ name: "RC1", timestamp: "2018-10-20T00:00:00Z" }],
      });

      visitQuestion(3);
      cy.wait("@getCollection");
      cy.findByTextEnsureVisible("Visualization");

      cy.findByLabelText("calendar icon").click();
      cy.findByText("Releases");
      sidebar().within(() => {
        cy.icon("ellipsis").click();
      });
      cy.findByTextEnsureVisible("Edit event").click();

      cy.findByLabelText("Event name")
        .clear()
        .type("RC2");
      cy.findByText("Update").click();
      cy.wait("@updateEvent");

      cy.findByTextEnsureVisible("Releases");
      cy.findByText("RC2");
    });

    it("should display all events in data view", () => {
      cy.createTimelineWithEvents({
        timeline: { name: "Releases" },
        events: [
          { name: "v1", timestamp: "2015-01-01T00:00:00Z" },
          { name: "v2", timestamp: "2017-01-01T00:00:00Z" },
          { name: "v3", timestamp: "2020-01-01T00:00:00Z" },
        ],
      });

      visitQuestion(3);
      cy.wait("@getCollection");
      cy.findByTextEnsureVisible("Visualization");

      cy.findByLabelText("calendar icon").click();
      cy.findByText("v1").should("not.exist");
      cy.findByText("v2").should("be.visible");
      cy.findByText("v3").should("be.visible");

      cy.findByLabelText("table2 icon").click();
      cy.findByText("v1").should("be.visible");
      cy.findByText("v2").should("be.visible");
      cy.findByText("v3").should("be.visible");
    });

    it("should archive and unarchive an event", () => {
      cy.createTimelineWithEvents({
        timeline: { name: "Releases" },
        events: [{ name: "RC1", timestamp: "2018-10-20T00:00:00Z" }],
      });

      visitQuestion(3);
      cy.wait("@getCollection");
      cy.findByTextEnsureVisible("Visualization");

      cy.findByLabelText("calendar icon").click();
      cy.findByText("Releases");
      sidebar().within(() => cy.icon("ellipsis").click());
      cy.findByTextEnsureVisible("Archive event").click();
      cy.wait("@updateEvent");
      cy.findByText("RC1").should("not.exist");

      cy.findByText("Undo").click();
      cy.wait("@updateEvent");
      cy.findByText("RC1");
    });

    it("should support markdown in event description", () => {
      cy.createTimelineWithEvents({
        timeline: {
          name: "Releases",
        },
        events: [
          {
            name: "RC1",
            description: "[Release notes](https://metabase.test)",
          },
        ],
      });

      visitQuestion(3);
      cy.findByLabelText("calendar icon").click();

      cy.findByText("Releases").should("be.visible");
      cy.findByText("Release notes").should("be.visible");
    });
  });

  describe("as readonly user", () => {
    it("should not allow creating default timelines", () => {
      cy.signIn("readonly");
      visitQuestion(3);
      cy.findByTextEnsureVisible("Created At");

      cy.findByLabelText("calendar icon").click();
      cy.findByTextEnsureVisible(/Events in Metabase/);
      cy.findByText("Add an event").should("not.exist");
    });

    it("should not allow creating or editing events", () => {
      cy.signInAsAdmin();
      cy.createTimelineWithEvents({
        timeline: { name: "Releases" },
        events: [{ name: "RC1" }],
      });
      cy.signOut();
      cy.signIn("readonly");
      visitQuestion(3);
      cy.findByTextEnsureVisible("Created At");

      cy.findByLabelText("calendar icon").click();
      cy.findByTextEnsureVisible("Releases");
      cy.findByText("Add an event").should("not.exist");
      sidebar().within(() => cy.icon("ellipsis").should("not.exist"));
    });
  });
});
