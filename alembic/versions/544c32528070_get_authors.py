"""Get authors

Revision ID: 544c32528070
Revises: 41ba7bdc2980
Create Date: 2016-03-02 19:11:33.026270

"""

# revision identifiers, used by Alembic.
revision = '544c32528070'
down_revision = '41ba7bdc2980'

from alembic import op
import sqlalchemy as sa

                               


def upgrade():
    op.add_column('records', sa.Column('authors', sa.Text))
    pass


def downgrade():
    op.drop_column('records', 'authors')
