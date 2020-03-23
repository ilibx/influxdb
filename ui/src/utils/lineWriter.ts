import AJAX from 'src/utils/ajax'

export interface Tags {
  [key: string]: string | number
}

export interface Fields {
  [key: string]: number | string
}

export enum Precision {
  ns = 'ns',
  u = 'u',
  ms = 'ms',
  s = 's',
  m = 'm',
  h = 'h',
}

const nowInSeconds = function nowInSeconds() {
  return Math.floor(Date.now() / 1000)
}

// Build a line writer that can write arbitrary line data
export const createLineFromModel = function createLineFromModel(
  measurement: string,
  tags: Tags,
  fields: Fields,
  timestamp: number
): string {
  let tagString = ''
  Object.keys(tags)
    // Sort keys for a little extra perf
    // https://v2.docs.influxdata.com/v2.0/write-data/best-practices/optimize-writes/#sort-tags-by-key
    .sort()
    .forEach((tagKey, i, tagKeys) => {
      const tagValue = tags[tagKey]
      tagString = `${tagString}${tagKey}=${tagValue}`

      // if this isn't the end of the string, append a comma
      if (i < tagKeys.length - 1) {
        tagString = `${tagString},`
      }
    })

  let fieldString = ''
  Object.keys(fields).forEach((fieldKey, i, fieldKeys) => {
    const fieldValue = fields[fieldKey]
    fieldString = `${fieldString}${fieldKey}=${fieldValue}`

    // if this isn't the end of the string, append a comma
    if (i < fieldKeys.length - 1) {
      fieldString = `${fieldString},`
    }
  })

  let lineStart = measurement
  if (tagString !== '') {
    lineStart = `${lineStart},${tagString}`
  }

  return `${lineStart} ${fieldString} ${timestamp}`
}

// Builds a basic line writer that will write a line to the url, org and bucket specified.
// Returns a function named `writeLine` that utilizes closures to access the configuration
// arguments passed in to `buildLineWriter`.
export const buildLineWriter = function buildLineWriter(
  url: string,
  orgId: number | string,
  bucketName: string,
  authToken: string
) {
  return async function writeLine(
    measurement: string,
    tags: Tags,
    fields: Fields,
    precision: Precision = Precision.s,
    timestamp: number = nowInSeconds()
  ) {
    const line = createLineFromModel(measurement, tags, fields, timestamp)

    const params = {
      org: orgId,
      bucket: bucketName,
      precision,
    }

    try {
      await AJAX(
        {
          method: 'POST',
          url: `${url}/api/v2/write`,
          data: line,
          headers: {
            Authorization: `Token ${authToken}`,
          },
          params,
        },
        true
      )
    } catch (error) {
      console.error(error)
    }
  }
}
