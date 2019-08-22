const fs = require('fs')
const JsonSocket = require('json-socket')
const moment = require('moment')
const parseString = require('xml2js').parseString
const crypto = require('crypto')

const sendToMariadbProxy = async (message) => {
  return JsonSocket.sendReceive({
    host: 'rish-mariadb-proxy',
    port: process.env.RISH_MARIADB_PROXY_PORT,
    delimeter: process.env.JSON_SOCKET_DELIMETER,
    message: message
	})
}

const sendToParser = async (message) => {
  return JsonSocket.sendReceive({
    host: 'rish-mdb-parser',
    port: process.env.RISH_MDB_PARSER_PORT,
    delimeter: process.env.JSON_SOCKET_DELIMETER,
    message: message
	})
}

const formatDateToGeneric = (string) => moment(string).format('YYYY-MM-DD')
const parseDateFromRus = (string) => moment(string, 'DD.MM.YYYY')
const convertDateToRus = (string) => moment(string).format('DD.MM.YYYY')
const convertRusDateToGeneric = (string) => parseDateFromRus(string).format('YYYY-MM-DD')
const convertDateTimeToRus = (string) =>  moment(string, "YYYY-MM-DD HH:mm:ss").format("DD.MM.YYYY HH:mm:ss")

const parseXML = (xml) => {
  return new Promise((resolve, reject) => {
    parseString(xml, (err, result) => {
      if (err) {
        reject([err])
      }
      else {
        resolve([null, result])
      }
    })
  })
}

const formatSNILS = (rawSnils) => {
  let result = null
  if (rawSnils) {
    const s = rawSnils.split("")
    result = `${s[0]}${s[1]}${s[2]}-${s[3]}${s[4]}${s[5]}-${s[6]}${s[7]}${s[8]} ${s[9]}${s[10]}`
  }
  return result
}

const normalizeValuesForSql = (array) => {
  return array.map(e => {
    if (typeof(e) === 'string' && e === "NULL") {
      return e
    }
    else if (typeof(e) === 'string' && (!e.startsWith("'") && !e.endsWith("'"))) {
      return `'${e}'`
    }
    else if (!e) {
      return 'NULL'
    }
    else {
      return e
    }
  })
}

const normalizeValueForSql = (e) => {
  if (typeof(e) === 'string' && e === "NULL") {
    return e
  }
  else if (typeof(e) === 'string' && (!e.startsWith("'") && !e.endsWith("'"))) {
    return `'${e}'`
  }
  else if (!e) {
    return 'NULL'
  }
  else {
    return e
  }
}

const formatDispObservationElementForTFOMS = (data) => {
  const tmp = data
  if (tmp.DATES && tmp.DATES !== '' && tmp.DATES.startsWith('[') && tmp.DATES.endsWith(']')) {
    tmp.DATES = JSON.parse(tmp.DATES).map(e => convertDateToRus(e))
  }
  tmp.UPDATED_AT = tmp.UPDATED_AT ? convertDateTimeToRus(tmp.UPDATED_AT) : null
  tmp.SNILS_VR = tmp.SNILS_VR ? formatSNILS(tmp.SNILS_VR) : null
  tmp.DATE_IN = tmp.DATE_IN ? convertDateToRus(tmp.DATE_IN) : null
  tmp.DATE_OUT = tmp.DATE_OUT ? convertDateToRus(tmp.DATE_OUT) : null
  return tmp
}

const formatDispObservationResponseToTFOMS = (qResult) => {
  //console.log(qResult);
  const result = {}
  result.error = qResult[0]
  result.data = null
  try {
    let data = qResult[1]
    let modifiedData = []
    if (data && data.length > 0) {
      for (let i = 0; i < data.length; i++) {
        modifiedData.push(formatDispObservationElementForTFOMS(data[i]))
      }
      result.data = modifiedData
    }
  } catch (e) {
    console.error(`Ошибка при форматировании ответа disp_observation: ${e}`)
  }
  return result
}

const sendNotification = async (message) => {
  return JsonSocket.sendReceive({
    host: 'rish-notifications',
    port: process.env.RISH_NOTIFICATIONS_PORT,
    delimeter: process.env.JSON_SOCKET_DELIMETER,
    message: message
	})
}

const sendNotificationToDispObservationErrors = (serviceName, text) => {
  const _text = `${serviceName} ${text}`
  console.error(_text)
  return JsonSocket.sendReceive({
    host: 'rish-notifications',
    port: process.env.RISH_NOTIFICATIONS_PORT,
    delimeter: process.env.JSON_SOCKET_DELIMETER,
    message: {
      type: 'dispObservation',
      subtype: 'errors',
      data: {
        text: _text
      }
    }
	})
}

const publishToQ = (_con, queue, message, callback) => {
  return _con.then((con) => {
    return con.createConfirmChannel()
    .then((ch) => {
      return ch.assertQueue(queue).then(_ok => {
        return ch.sendToQueue(queue, Buffer.from(message), {persistent: true}, (err, ok) => {
          callback(err, ok)
        })
      })
    })
  }).then(e => console.warn(e))
}

const consumeFromQ = (_con, queue, callback) => {
  return _con.then((con) => {
    return con.createChannel()
    .then(ch => {
      ch.prefetch(1)
      ch.consume(queue, (RMQmsg) => {
        callback(ch, RMQmsg)
      })
    })
  }).catch(e => console.warn(e))
}

const logErrorInService = async (error, opts, SERVICE_NAME) => {
  try {
    const _message = `Ошибка ${error.stack}, запрос: ${JSON.stringify(opts.message)}`
    await sendNotificationToDispObservationErrors(SERVICE_NAME, _message)
    opts.socket.sendEndMessage([error])
  } catch (e) {
    console.error(e)
  }
}
// { type: 'event|error|stats', text: 'текст', SEEVICE_NAME: 'имя_сервиса'}
const logAndSendToTgChannel = async (opts) => {
  try {
    const _message = `${opts.SERVICE_NAME}: ${opts.type === 'error' ? 'ошибка': ''} ${opts.text}`
    if (opts.type === 'error') {
      console.error(_message)
    }
    else {
      console.log(_message)
    }
    await JsonSocket.sendReceive({
      host: 'rish-notifications',
      port: process.env.RISH_NOTIFICATIONS_PORT,
      delimeter: process.env.JSON_SOCKET_DELIMETER,
      message: {
        type: opts.type,
        data: {
          text: _message
        }
      }
    })
  } catch (e) {
    console.error(`${SERVICE_NAME}: ошибка при отправке уведомления ${e}.`)
  }
}

const getHash = (string) => crypto.createHash('sha256').update(string, 'utf8').digest('hex')

module.exports.publishToQ = publishToQ
module.exports.consumeFromQ = consumeFromQ
module.exports.sendToParser = sendToParser
module.exports.sendToMariadbProxy = sendToMariadbProxy
module.exports.formatDateToGeneric = formatDateToGeneric
module.exports.parseDateFromRus = parseDateFromRus
module.exports.convertRusDateToGeneric = convertRusDateToGeneric
module.exports.parseXML = parseXML
module.exports.formatSNILS = formatSNILS
module.exports.normalizeValuesForSql = normalizeValuesForSql
module.exports.normalizeValueForSql = normalizeValueForSql
module.exports.convertDateToRus = convertDateToRus
module.exports.convertDateTimeToRus = convertDateTimeToRus
module.exports.formatDispObservationResponseToTFOMS = formatDispObservationResponseToTFOMS
module.exports.formatDispObservationElementForTFOMS = formatDispObservationElementForTFOMS
module.exports.sendNotification = sendNotification
module.exports.sendNotificationToDispObservationErrors = sendNotificationToDispObservationErrors
module.exports.logErrorInService = logErrorInService
module.exports.getHash = getHash
module.exports.logAndSendToTgChannel = logAndSendToTgChannel
