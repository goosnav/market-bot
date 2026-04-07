"""Repository exports for the Sprint 2 persistence layer."""

from backend.app.repositories.audit import AuditEventRepository
from backend.app.repositories.base import read_session, write_session
from backend.app.repositories.builder import (
    CampaignAudienceSnapshotRepository,
    CampaignPreviewRepository,
    ProviderAccountRepository,
    SequenceRepository,
)
from backend.app.repositories.campaigns import CampaignRepository
from backend.app.repositories.execution import ExecutionRepository
from backend.app.repositories.leads import CompanyRepository, LeadRepository
from backend.app.repositories.queue import QueuedMessageRepository, ReplyRepository
from backend.app.repositories.studio import (
    GenerationArtifactRepository,
    OfferProfileRepository,
    TemplateBlockRepository,
    TemplateRepository,
    TemplateVariantRepository,
    VerticalPlaybookRepository,
)
from backend.app.repositories.warehouse import ImportJobRepository, ListRepository, SavedFilterRepository, TagRepository

__all__ = [
    "AuditEventRepository",
    "CampaignAudienceSnapshotRepository",
    "CampaignPreviewRepository",
    "CampaignRepository",
    "CompanyRepository",
    "ExecutionRepository",
    "GenerationArtifactRepository",
    "ImportJobRepository",
    "LeadRepository",
    "ListRepository",
    "OfferProfileRepository",
    "ProviderAccountRepository",
    "QueuedMessageRepository",
    "ReplyRepository",
    "SavedFilterRepository",
    "SequenceRepository",
    "TagRepository",
    "TemplateBlockRepository",
    "TemplateRepository",
    "TemplateVariantRepository",
    "VerticalPlaybookRepository",
    "read_session",
    "write_session",
]
