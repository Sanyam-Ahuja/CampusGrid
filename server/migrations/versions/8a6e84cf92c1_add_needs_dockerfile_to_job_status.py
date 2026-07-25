"""add needs_dockerfile to job_status

Revision ID: 8a6e84cf92c1
Revises: 786f26d5b694
Create Date: 2026-07-25 11:00:00.000000
"""
from typing import Sequence, Union
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '8a6e84cf92c1'
down_revision: Union[str, None] = '786f26d5b694'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Use autocommit block because ALTER TYPE ADD VALUE cannot run inside a transaction in Postgres
    with op.get_context().autocommit_block():
        op.execute("ALTER TYPE job_status ADD VALUE 'NEEDS_DOCKERFILE'")


def downgrade() -> None:
    # Note: PostgreSQL does not support removing values from an ENUM type.
    pass
