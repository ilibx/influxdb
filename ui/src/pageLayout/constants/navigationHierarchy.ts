import {IconFont} from '@influxdata/clockface'
import {
  CLOUD_URL,
  CLOUD_USERS_PATH,
  CLOUD_USAGE_PATH,
  CLOUD_BILLING_PATH,
} from 'src/shared/constants'

export interface NavSubItem {
  id: string
  testID: string
  label: string
  link: string
  cloudExclude?: boolean
  cloudOnly?: boolean
  featureFlag?: string
}

export interface NavItem {
  id: string
  testID: string
  label: string
  link: string
  icon: IconFont
  cloudExclude?: boolean
  cloudOnly?: boolean
  featureFlag?: string
  menu?: NavSubItem[]
  activeKeywords: string[]
}

export const generateNavItems = (orgID: string): NavItem[] => {
  const orgPrefix = `/orgs/${orgID}`

  return [
    {
      id: 'load-data',
      testID: 'nav-item-load-data',
      icon: IconFont.DisksNav,
      label: 'Data',
      link: `${orgPrefix}/load-data/buckets`,
      activeKeywords: ['load-data'],
      menu: [
        {
          id: 'buckets',
          testID: 'nav-subitem-buckets',
          label: 'Buckets',
          link: `${orgPrefix}/load-data/buckets`,
        },
        {
          id: 'telegrafs',
          testID: 'nav-subitem-telegrafs',
          label: 'Telegraf',
          link: `${orgPrefix}/load-data/telegrafs`,
        },
        {
          id: 'scrapers',
          testID: 'nav-subitem-scrapers',
          label: 'Scrapers',
          link: `${orgPrefix}/load-data/scrapers`,
          cloudExclude: true,
        },
        {
          id: 'tokens',
          testID: 'nav-subitem-tokens',
          label: 'Tokens',
          link: `${orgPrefix}/load-data/tokens`,
        },
        {
          id: 'client-libraries',
          testID: 'nav-subitem-client-libraries',
          label: 'Client Libraries',
          link: `${orgPrefix}/load-data/client-libraries`,
        },
      ],
    },
    {
      id: 'data-explorer',
      testID: 'nav-item-data-explorer',
      icon: IconFont.GraphLine,
      label: 'Explore',
      link: `${orgPrefix}/data-explorer`,
      activeKeywords: ['data-explorer'],
    },
    {
      id: 'org-cloud',
      testID: 'nav-item-org-cloud',
      icon: IconFont.UsersTrio,
      label: 'Org',
      link: CLOUD_USAGE_PATH,
      cloudOnly: true,
      activeKeywords: ['org'],
      menu: [
        {
          id: 'usage',
          testID: 'nav-subitem-usage',
          label: 'Usage',
          link: CLOUD_USAGE_PATH,
        },
        {
          id: 'billing',
          testID: 'nav-subitem-billing',
          label: 'Billing',
          link: CLOUD_BILLING_PATH,
        },
        {
          id: 'multi-user-members',
          testID: 'nav-subitem-multi-user-members',
          label: 'Members',
          featureFlag: 'multiUser',
          link: `${CLOUD_URL}/organizations/${orgID}${CLOUD_USERS_PATH}`,
        },
        {
          id: 'about',
          testID: 'nav-subitem-about',
          label: 'About',
          link: `${orgPrefix}/about`,
        },
      ],
    },
    {
      id: 'org-oss',
      testID: 'nav-item-org-oss',
      icon: IconFont.UsersTrio,
      label: 'Org',
      link: `${orgPrefix}/members`,
      cloudExclude: true,
      activeKeywords: ['members', 'about'],
      menu: [
        {
          id: 'members',
          testID: 'nav-subitem-members',
          label: 'Members',
          link: `${orgPrefix}/members`,
          cloudExclude: true,
        },
        {
          id: 'multi-user-members',
          testID: 'nav-subitem-multi-user-members',
          label: 'Members',
          featureFlag: 'multiUser',
          link: `${CLOUD_URL}/organizations/${orgID}/${CLOUD_USERS_PATH}`,
        },
        {
          id: 'about',
          testID: 'nav-subitem-about',
          label: 'About',
          link: `${orgPrefix}/about`,
        },
      ],
    },
    {
      id: 'dashboards',
      testID: 'nav-item-dashboards',
      icon: IconFont.Dashboards,
      label: 'Boards',
      link: `${orgPrefix}/dashboards`,
      activeKeywords: ['dashboards'],
    },
    {
      id: 'tasks',
      testID: 'nav-item-tasks',
      icon: IconFont.Calendar,
      label: 'Tasks',
      link: `${orgPrefix}/tasks`,
      activeKeywords: ['tasks'],
    },
    {
      id: 'alerting',
      testID: 'nav-item-alerting',
      icon: IconFont.Bell,
      label: 'Alerts',
      link: `${orgPrefix}/alerting`,
      activeKeywords: ['alerting'],
      menu: [
        {
          id: 'history',
          testID: 'nav-subitem-history',
          label: 'Alert History',
          link: `${orgPrefix}/alert-history`,
        },
      ],
    },
    {
      id: 'settings',
      testID: 'nav-item-settings',
      icon: IconFont.WrenchNav,
      label: 'Settings',
      link: `${orgPrefix}/settings/variables`,
      activeKeywords: ['settings'],
      menu: [
        {
          id: 'variables',
          testID: 'nav-subitem-variables',
          label: 'Variables',
          link: `${orgPrefix}/settings/variables`,
        },
        {
          id: 'templates',
          testID: 'nav-subitem-templates',
          label: 'Templates',
          link: `${orgPrefix}/settings/templates`,
        },
        {
          id: 'labels',
          testID: 'nav-subitem-labels',
          label: 'Labels',
          link: `${orgPrefix}/settings/labels`,
        },
      ],
    },
  ]
}
