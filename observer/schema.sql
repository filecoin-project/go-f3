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
