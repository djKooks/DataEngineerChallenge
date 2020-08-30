package com.detest

import java.sql.Timestamp

case class ELBLogFormat (
    timestamp: Timestamp,
    elb: String,
    client: String,
    backend: String,
    elbStatusCode: Int,
    backendStatusCode: String,
    request: String,
    userAgent: String,
    sslCipher: String,
    sslProtocol: String,
    interval: Long,
    newSessionFlag: Boolean,
    sessionKey: Long,
    session: String
)
