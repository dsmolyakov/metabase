import type { Field } from "./field";
import type { DatasetQuery } from "./query";

export interface Card extends UnsavedCard {
  id: CardId;
  collection_id: number | null;
  name: string;
  description: string | null;
  dataset: boolean;
  can_write: boolean;
  cache_ttl: number | null;
  query_average_duration?: number | null;
  last_query_start: string | null;
  result_metadata: Field[];
  archived: boolean;

  creator?: {
    id: number;
    common_name: string;
    first_name: string | null;
    last_name: string | null;
    email: string;
    last_login: string;
    date_joined: string;
  };
}

export interface UnsavedCard {
  display: string;
  dataset_query: DatasetQuery;
  visualization_settings: VisualizationSettings;
}

export type SeriesSettings = {
  title: string;
  color?: string;
  show_series_values?: boolean;
};

export type SeriesOrderSetting = {
  name: string;
  key: string;
  enabled: boolean;
  color?: string;
};

export type VisualizationSettings = {
  "graph.show_values"?: boolean;
  "stackable.stack_type"?: "stacked" | "normalized" | null;

  // X-axis
  "graph.x_axis.title_text"?: string;
  "graph.x_axis.scale"?: "ordinal";
  "graph.x_axis.axis_enabled"?: "compact";

  // Y-axis
  "graph.y_axis.title_text"?: string;
  "graph.y_axis.scale"?: "linear" | "pow" | "log";
  "graph.y_axis.axis_enabled"?: true;

  // Goal
  "graph.goal_value"?: number;
  "graph.show_goal"?: boolean;
  "graph.goal_label"?: string;

  // Series
  "graph.dimensions"?: string[];
  "graph.metrics"?: string[];

  // Series settings
  series_settings?: Record<string, SeriesSettings>;

  "graph.series_order"?: SeriesOrderSetting[];

  // Funnel settings
  "funnel.rows"?: SeriesOrderSetting[];

  [key: string]: any;
};

export interface ModerationReview {
  moderator_id: number;
  status: ModerationReviewStatus | null;
  created_at: string;
  most_recent: boolean;
}

export type CardId = number;
export type ModerationReviewStatus = "verified";
