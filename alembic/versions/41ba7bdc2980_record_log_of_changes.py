"""Record log of changes

Revision ID: 41ba7bdc2980
Revises: 456fd4e10658
Create Date: 2016-02-25 16:43:00.645942

"""

# revision identifiers, used by Alembic.
revision = '41ba7bdc2980'
down_revision = '456fd4e10658'

from alembic import op
import sqlalchemy as sa
import datetime
                               


def upgrade():
    op.create_table('change_log',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('created', sa.TIMESTAMP, default=datetime.datetime.utcnow),
        sa.Column('key', sa.String(255), nullable=False),
        sa.Column('oldvalue', sa.Text),
        sa.Column('oldvalue', sa.Text)
    )


def downgrade():
    op.drop_table('change_log')
