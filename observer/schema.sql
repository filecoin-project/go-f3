-- TODO: define a PAYLOAD type and refactor the duplicate STRUCT type definition for gpbft.Payload.
--       Note that TYPE in duckdb does not support IF NOT EXISTS clause so there is a need to check
--       if type exits and only create it if it does not... which is more hassle than it's worth.
--       Hence, the duplicate STRUCT definition of Payload.

CREATE TABLE IF NOT EXISTS latest_messages (
  Timestamp TIMESTAMP,
  NetworkName VARCHAR,
  Sender BIGINT,
  Vote STRUCT(
    Instance BIGINT,
    Round BIGINT,
    Phase ENUM(
      'INITIAL',
      'QUALITY',
      'CONVERGE',
      'PREPARE',
      'COMMIT',
      'DECIDE',
      'TERMINATED'
    ),
    SupplementalData STRUCT(
      Commitments BLOB,
      PowerTable VARCHAR
    ),
    Value STRUCT(
      Epoch BIGINT,
      Key BLOB,
      Commitments BLOB,
      PowerTable VARCHAR
    )[]
  ),
  Signature BLOB,
  Ticket BLOB,
  Justification STRUCT(
    Vote STRUCT(
      Instance BIGINT,
      Round BIGINT,
      Phase ENUM(
        'INITIAL',
        'QUALITY',
        'CONVERGE',
        'PREPARE',
        'COMMIT',
        'DECIDE',
        'TERMINATED'
      ),
      SupplementalData STRUCT(
        Commitments BLOB,
        PowerTable VARCHAR
      ),
      Value STRUCT(
        Epoch BIGINT,
        Key BLOB,
        Commitments BLOB,
        PowerTable VARCHAR
      )[]
    ),
    Signers BIGINT[],
    Signature BLOB
  ) NULL
);

-- Add Key column to latest_messages table to accommodate partial messages.
ALTER TABLE latest_messages
ADD COLUMN IF NOT EXISTS VoteValueKey BLOB;


CREATE TABLE IF NOT EXISTS finality_certificates (
  Timestamp TIMESTAMP,
  NetworkName VARCHAR,
  Instance BIGINT,
  ECChain STRUCT(
    Epoch BIGINT,
    Key BLOB,
    Commitments BLOB,
    PowerTable VARCHAR
  )[],
  SupplementalData STRUCT(
    Commitments BLOB,
    PowerTable VARCHAR
  ),
  Signers BIGINT[],
  Signature BLOB,
  PowerTableDelta STRUCT(
    ParticipantID BIGINT,
    PowerDelta BIGINT,
    SigningKey BLOB
  )[],
  PRIMARY KEY (NetworkName, Instance)
);

CREATE TABLE IF NOT EXISTS chain_exchanges (
  Timestamp TIMESTAMP,
  NetworkName VARCHAR,
  Instance BIGINT,
  VoteValueKey BLOB,
  VoteValue STRUCT(
    Epoch BIGINT,
    Key BLOB,
    Commitments BLOB,
    PowerTable VARCHAR
  )[],
  PRIMARY KEY (NetworkName, Instance, VoteValueKey)
);
