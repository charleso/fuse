package fuse

/**
 * Represents a remote path that Spark can handle.
 *
 * When running Spark on Yarn, this will default to HDFS by default, but `s3` or `file` protocol schemes can be used.
 *
 * NOTE: This should _not_ be used to represent local files, unless the value is guaranteed to be formatted correctly.
 */
case class RemotePath(value: String) extends AnyVal
