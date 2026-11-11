-- Mesmo tratamento para contas: o versionamento é feito explicitamente no passo 04.
ALTER TABLE `desafio.public.accounts` SET (
  'value.format'   = 'avro-registry',
  'changelog.mode' = 'append'
);
