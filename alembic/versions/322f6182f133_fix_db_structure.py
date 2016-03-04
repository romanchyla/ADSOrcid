"""Fix db structure

Revision ID: 322f6182f133
Revises: 544c32528070
Create Date: 2016-03-04 17:07:50.535797

"""

# revision identifiers, used by Alembic.
revision = '322f6182f133'
down_revision = '544c32528070'

from alembic import op
import sqlalchemy as sa

                               


def upgrade():
    op.add_column('change_log', sa.Column('newvalue', sa.Text))

def downgrade():
    pass
