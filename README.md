Feedback Service
An event-driven microservice that receives test submissions via Kafka, generates feedback using LLM model gateway, and stores results in MongoDB following Clean Architecture principles.

Architecture Overview
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Test System   │───▶│  Kafka Topics    │───▶│ Feedback Service│
│                 │    │                  │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                        │
                       ┌─────────────────┐              │
                       │   LLM Gateway   │◀─────────────┘
                       │                 │
                       └─────────────────┘
