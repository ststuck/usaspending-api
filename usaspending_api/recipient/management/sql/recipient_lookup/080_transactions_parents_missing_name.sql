DO $$ BEGIN RAISE NOTICE '080 Adding Recipient records from FPDS and FABS parents with no name'; END $$;

INSERT INTO public.temporary_restock_recipient_lookup (
  recipient_hash,
  legal_business_name,
  duns,
  uei,
  source,
  parent_duns,
  parent_legal_business_name
)
SELECT
  DISTINCT ON (parent_recipient_hash)
  parent_recipient_hash AS recipient_hash,
  ultimate_parent_legal_enti,
  ultimate_parent_unique_ide,
  ultimate_parent_uei as uei,
  CONCAT(source, '-parent'),
  ultimate_parent_unique_ide AS parent_duns,
  UPPER(ultimate_parent_legal_enti) AS parent_legal_business_name
FROM public.temporary_transaction_recipients_view
WHERE ultimate_parent_unique_ide IS NOT NULL AND ultimate_parent_legal_enti IS NULL
ORDER BY parent_recipient_hash, action_date DESC, is_fpds, transaction_unique_id
ON CONFLICT (recipient_hash) DO NOTHING;
