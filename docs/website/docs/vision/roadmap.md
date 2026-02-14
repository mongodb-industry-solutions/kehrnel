---
sidebar_position: 1
---

# Vision & Roadmap

## Vision

Build an open healthcare data ecosystem runtime where teams can:

- model heterogeneous healthcare documents
- transform and query across standards and custom schemas
- operate trustworthy pipelines with explicit contracts
- combine deterministic engineering and AI assistance responsibly

## Strategic Direction

### 1. Multi-Strategy, Multi-Domain Core

\{kehrnel\} remains strategy-pack driven, with domain-specific adapters and environment-scoped activation.

### 2. Beyond Structured-Only Data

Roadmap includes unstructured clinical report and extract processing, including:

- OCR/text normalization where needed
- section-aware extraction
- schema-constrained output generation

### 3. Hybrid Mapping (Rules + LLM)

Use deterministic rules and LLM components together:

- rules for stable, auditable mappings
- LLMs for fuzzy extraction and semantic interpretation
- validation layers for consistency and safety

### 4. Golden Record Pipelines

Support end-to-end generation of high-quality records:

- extract
- normalize
- validate
- persist
- expose via query and API interfaces

### 5. Open and Portable Architecture

\{kehrnel\} is designed to be:

- API-first and inspectable
- persistence-extensible (MongoDB-first today)
- compatible with open standards and custom models

### 6. AI-Ready Data Access

\{kehrnel\} is evolving toward agentic and AI-native data operations where autonomous systems can access data at scale with explicit policy controls, auditability, and governed execution boundaries.

## Current State

Today, the most mature production path is openEHR on MongoDB with the `openehr.rps_dual` reference strategy.

This is the starting point, not the product boundary.
