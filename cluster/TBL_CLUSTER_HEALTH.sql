-- Drafted on request by custom private GPT, 2025-09-29

-- Admin table to keep history
create database if not exists ADMIN;

create schema if not exists MONITORING;

create or replace table MONITORING.CLUSTER_HEALTH (
  measured_at        timestamp_ltz,
  table_catalog      string,
  table_schema       string,
  table_name         string,
  clustering_key     string,
  auto_clustering_on boolean,
  avg_depth          float,
  info               variant,     -- full SYSTEM$CLUSTERING_INFORMATION payload
  ac_credits_7d      number(38,3) -- credits auto-clustering used in last 7 days
);
