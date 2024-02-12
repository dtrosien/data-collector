-- noinspection SqlNoDataSourceInspectionForFile

-- Add migration script here
alter table countries add column is_sp500_eligible boolean not null default false;

-- List according to methodology_sp500 Appendix A page 27 (Some are missing)
update  countries set is_sp500_eligible = true where 
country_code in ('ABW','AIA','ATG','BHS','BLM','BMU','BRB','CYM','DMA','DOM','GIB','GRD','HTI','IMN','JAM','KNA','LBR','LCA','LUX','MAF','MSR','PAN','PRI','TCA','TTO','USA','VCT','VGB','VIR');

insert INTO public.countries (country_code, nation, federal_state, sec_code, is_sp500_eligible) VALUES('CUW', 'Curaçao', '', '', true) ON CONFLICT DO NOTHING;