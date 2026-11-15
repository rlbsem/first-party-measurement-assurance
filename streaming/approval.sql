-- BigQuery DML only: no transaction is claimed across the broker or processing service.
BEGIN TRANSACTION;
ASSERT (SELECT COUNT(*) FROM `{output}.approvals` WHERE request=@request)<=1 AS 'duplicate approval receipts';
IF EXISTS(SELECT 1 FROM `{output}.approvals` WHERE request=@request) THEN
 ASSERT (SELECT COUNT(*) FROM `{output}.approvals` WHERE request=@request AND content_hash=@content_hash)=1 AS 'approval request conflict';
ELSE
 ASSERT (SELECT COUNT(*) FROM `{output}.candidates` c JOIN `{output}.proofs` p
  USING(candidate,source_hash,output_hash,policy)
  WHERE c.candidate=@candidate AND c.tenant=@tenant AND p.verified AND c.policy=@policy AND c.lane=@lane AND c.cohort=@cohort)=1 AS 'verified candidate required';
 ASSERT ({root_select})=
  (SELECT output_hash FROM `{output}.candidates` WHERE candidate=@candidate AND tenant=@tenant) AS 'candidate bytes changed';
 ASSERT (SELECT COUNT(*) FROM `{output}.candidate_events` WHERE candidate=@candidate AND tenant=@tenant)=
  (SELECT row_count FROM `{output}.candidates` WHERE candidate=@candidate AND tenant=@tenant) AS 'candidate membership changed';
 UPDATE `{output}.selectors` SET candidate=@candidate,epoch=epoch+1
  WHERE tenant=@tenant AND lane=@lane AND cohort=@cohort AND epoch=@expected_epoch AND policy=@policy AND source_hash=(SELECT source_hash FROM `{output}.candidates` WHERE candidate=@candidate);
 ASSERT @@row_count=1 AS 'stale predecessor or policy';
 INSERT `{output}.approvals`(request,content_hash,candidate,tenant,epoch,lane,cohort)
 VALUES(@request,@content_hash,@candidate,@tenant,@expected_epoch+1,@lane,@cohort);
END IF;
COMMIT TRANSACTION;
SELECT request,candidate,tenant,epoch FROM `{output}.approvals` WHERE request=@request;
