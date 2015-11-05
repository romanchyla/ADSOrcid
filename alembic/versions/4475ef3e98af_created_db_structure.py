"""created db structure

Revision ID: 4475ef3e98af
Revises: 466986a461f1
Create Date: 2015-11-04 11:09:19.590444

"""

# revision identifiers, used by Alembic.
revision = '4475ef3e98af'
down_revision = None

from alembic import op
import sqlalchemy as sa

import datetime
                               

from sqlalchemy import Column, String, Integer, TIMESTAMP, DateTime, Text, Index
from sqlalchemy.sql import table, column


def upgrade():
    op.create_table('authors',
        Column('id', Integer, primary_key=True),
        Column('orcidid', String(19), unique=True, nullable=False),
        Column('name', String(255)),
        Column('facts', Text),
        Column('status', String(255)),
        Column('account_id', Integer),
        Column('created', TIMESTAMP, default=datetime.datetime.utcnow),
        Column('updated', TIMESTAMP, default=datetime.datetime.utcnow),
        Index('ix_updated', 'updated')
    )
    
    op.create_table('claims',
        Column('id', Integer, primary_key=True),
        Column('orcidid', String(19), nullable=False),
        Column('bibcode', String(19), nullable=False),
        Column('status', String(255)),
        Column('provenance', String(255)),
        Column('created', TIMESTAMP, default=datetime.datetime.utcnow),
        Index('ix_created', 'created'),
        Index('ix_orcidid', 'orcidid'),
        Index('ix_bibcode', 'bibcode')
    )
    
    op.create_table('records',
        Column('id', Integer, primary_key=True),
        Column('bibcode', String(19), unique=True, nullable=False),
        Column('status', String(255)),
        Column('claims', Text),
        Column('created', TIMESTAMP),
        Column('updated', TIMESTAMP, default=datetime.datetime.utcnow),
        Column('processed', TIMESTAMP),
        Index('ix_recs_updated', 'updated'),
        Index('ix_recs_created', 'created'),
        Index('ix_processed', 'processed')
    )

def downgrade():
    op.drop_table('authors')
    op.drop_table('claims')
    op.drop_table('records')
