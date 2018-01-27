import React, {PropTypes} from 'react'

import Tags from 'shared/components/Tags'
import SlideToggle from 'shared/components/SlideToggle'
import ConfirmButton from 'shared/components/ConfirmButton'

import {ALL_USERS_TABLE} from 'src/admin/constants/chronografTableSizing'
const {
  colOrganizations,
  colProvider,
  colScheme,
  colSuperAdmin,
  colActions,
} = ALL_USERS_TABLE

const AllUsersTableRow = ({
  organizations,
  user,
  onAddToOrganization,
  onRemoveFromOrganization,
  onChangeSuperAdmin,
  onDelete,
  meID,
}) => {
  const dropdownOrganizationsItems = organizations.map(r => ({
    ...r,
    text: r.name,
  }))

  const userIsMe = user.id === meID

  const userOrganizations = user.roles.map(r => ({
    ...r,
    name: organizations.find(o => r.organization === o.id).name,
  }))

  const wrappedDelete = () => onDelete(user)

  return (
    <tr className={'chronograf-admin-table--user'}>
      <td>
        {userIsMe
          ? <strong className="chronograf-user--me">
              <span className="icon user" />
              {user.name}
            </strong>
          : <strong>
              {user.name}
            </strong>}
      </td>
      <td style={{width: colOrganizations}}>
        <Tags
          tags={userOrganizations}
          onDeleteTag={onRemoveFromOrganization(user)}
          emptyStateText="None"
          addMenuItems={dropdownOrganizationsItems}
          addMenuChoose={onAddToOrganization(user)}
        />
      </td>
      <td style={{width: colProvider}}>
        {user.provider}
      </td>
      <td style={{width: colScheme}}>
        {user.scheme}
      </td>
      <td style={{width: colSuperAdmin}} className="text-center">
        <SlideToggle
          active={user.superAdmin}
          onToggle={onChangeSuperAdmin(user)}
          size="xs"
          disabled={userIsMe}
        />
      </td>
      <td style={{textAlign: 'right', width: colActions}}>
        <ConfirmButton
          confirmText="Remove from all Orgs"
          confirmAction={wrappedDelete}
          size="btn-xs"
          text="Remove"
          disabled={userIsMe}
          customClass="table--show-on-row-hover"
        />
      </td>
    </tr>
  )
}

const {arrayOf, func, shape, string} = PropTypes

AllUsersTableRow.propTypes = {
  user: shape(),
  organization: shape({
    name: string.isRequired,
    id: string.isRequired,
  }),
  onAddToOrganization: func.isRequired,
  onRemoveFromOrganization: func.isRequired,
  onChangeSuperAdmin: func.isRequired,
  onDelete: func.isRequired,
  meID: string.isRequired,
  organizations: arrayOf(
    shape({
      id: string.isRequired,
      name: string.isRequired,
    })
  ),
}

export default AllUsersTableRow
