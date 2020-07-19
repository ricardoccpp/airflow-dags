drop table if exists stage.stg_exam_results_full;

create table stage.stg_exam_results_full (
	na_instituicao varchar(100) not null,
	id_paciente varchar(50) not null,
	id_atendimento varchar(50) null,
	dt_coleta date not null,
	de_origem varchar(50) null,
	de_exame text null,
	de_analito text null,
	de_resultado text null,
	cd_unidade text null,
	de_valor_referencia text null,
	cd_tipo_comparacao varchar(50) null,
	nu_limite_inferior decimal(38,10) null,
	nu_limite_superior decimal(38,10) null,
	nu_referencia_unica decimal(38,10) null,
	cd_referencia_exata varchar(500) null,
	fl_teste_covid19 boolean,
	ds_resultado_comparacao varchar(50) null,
	dt_load timestamptz not null
);

create index idx_stg_exam_results_na_instituicao on stage.stg_exam_results_full(na_instituicao);
create index idx_stg_exam_results_id_paciente on stage.stg_exam_results_full(id_paciente);
create index idx_stg_exam_results_id_paciente_na_instituicao on stage.stg_exam_results_full(id_paciente, na_instituicao);
create index idx_stg_exam_results_dt_coleta on stage.stg_exam_results_full(dt_coleta);
create index idx_stg_exam_results_fl_teste_covid19 on stage.stg_exam_results_full(fl_teste_covid19);

insert into stage.stg_exam_results_full
select
	na_instituicao,
	id_paciente,
	id_atendimento,
	dt_coleta,
	de_origem,
	de_exame,
	de_analito,
	de_resultado,
	cd_unidade,
	de_valor_referencia,
	null as cd_tipo_comparacao,
	null as nu_limite_inferior,
	null as nu_limite_superior,
	null as nu_referencia_unica,
	null as cd_referencia_exata,
	null as fl_teste_covid19,
	null as ds_resultado_comparacao,
	dt_load
from (
	select
		id_paciente,
		'Not applied' as id_atendimento,
		'Einstein' as na_instituicao,
		to_date(dt_coleta, 'dd/mm/yyyy') as dt_coleta,
		trim(de_origem) as de_origem,
		trim(de_exame) as de_exame,
		trim(de_analito) as de_analito,
		trim(de_resultado) as de_resultado,
		trim(cd_unidade) as cd_unidade,
		trim(de_valor_referencia) as de_valor_referencia,
		now() as dt_load
	from stage.stg_exam_results_einstein
	union
	select
		id_paciente,
		'Not applied' as id_atendimento,
		'Fleury' as na_instituicao,
		to_date(dt_coleta, 'dd/mm/yyyy') as dt_coleta,
		trim(de_origem) as de_origem,
		trim(de_exame) as de_exame,
		trim(de_analito) as de_analito,
		trim(de_resultado) as de_resultado,
		trim(cd_unidade) as cd_unidade,
		trim(de_valor_referencia) as de_valor_referencia,
		now() as dt_load
	from stage.stg_exam_results_fleury
	union
	select
		id_paciente,
		id_atendimento,
		'Sírio Libanês' as na_instituicao,
		to_date(dt_coleta, 'yyyy-mm-dd') as dt_coleta,
		trim(de_origem) as de_origem,
		trim(de_exame) as de_exame,
		trim(de_analito) as de_analito,
		trim(de_resultado) as de_resultado,
		trim(cd_unidade) as cd_unidade,
		trim(de_valor_referencia) as de_valor_referencia,
		now() as dt_load
	from stage.stg_exam_results_sirio
) x
order by 1, 2, 3, 4
;

----------------------------------------------------------------------
update stage.stg_exam_results_full
set
	cd_tipo_comparacao = case when de_valor_referencia ilike 'inferior a %%'
							then 'less than'
							else 'equal to'
						end,
	nu_limite_inferior = case when de_valor_referencia ilike 'inferior a %%'
							then null
						end::decimal,
	nu_limite_superior = case when de_valor_referencia ilike 'inferior a %%'
							then null
						end::decimal,
	nu_referencia_unica = case when de_valor_referencia ilike 'inferior a %%'
						then trim(replace(replace(de_valor_referencia, 'inferior a ', ''), ',', '.'))
						else null
					end::decimal,
	cd_referencia_exata = case when de_valor_referencia ilike 'inferior a %%'
							then null
							else upper(de_valor_referencia)
						end,
	fl_teste_covid19 = true
where 1 = 1
	and na_instituicao = 'Fleury'
	and de_analito ilike '%%covid%%'
	and de_valor_referencia is not null
;

update stage.stg_exam_results_full
set
	ds_resultado_comparacao = case cd_tipo_comparacao when 'equal to'
									then case when upper(de_resultado) = cd_referencia_exata
										then 'NORMAL'
										else 'ALTERADO'
									end
								when 'less than or equal to'
									then case when trim(replace(regexp_replace(de_resultado, '[A-z]*', '', 'g'), ',', '.'))::decimal < nu_referencia_unica
										then 'NORMAL'
										else 'ALTERADO'
									end
								else null
							end
where 1 = 1
	and na_instituicao = 'Fleury'
	and de_analito ilike '%%covid%%'
	and de_valor_referencia is not null
;
----------------------------------------------------------------------
----------------------------------------------------------------------
update stage.stg_exam_results_full
set
	cd_tipo_comparacao = case when de_valor_referencia ilike '<=%%'
							then 'less than or equal to'
							else 'equal to'
						end,
	nu_limite_inferior = case when de_valor_referencia ilike '<=%%'
							then null
						end::decimal,
	nu_limite_superior = case when de_valor_referencia ilike '<=%%'
							then null
						end::decimal,
	nu_referencia_unica = case when de_valor_referencia ilike '<=%%'
						then trim(replace(replace(de_valor_referencia, '<=', ''), ',', '.'))::decimal
						else null
					end::decimal,
	cd_referencia_exata = case when de_valor_referencia ilike '<=%%'
							then null
							else upper(de_valor_referencia)
						end,
	fl_teste_covid19 = true
where 1 = 1
	and na_instituicao = 'Einstein'
	and de_analito ilike '%%covid%%'
	and de_valor_referencia is not null
;

update stage.stg_exam_results_full
set
	ds_resultado_comparacao = case cd_tipo_comparacao when 'equal to'
									then case when upper(de_resultado) = cd_referencia_exata
										then 'NORMAL'
										else 'ALTERADO'
									end
								when 'less than or equal to'
									then case when trim(replace(replace(replace(regexp_replace(de_resultado, '[A-z]*', '', 'g'), ',', '.'), 'ã', ''), '>', '')) = ''
											then null
											else case when trim(replace(replace(replace(regexp_replace(de_resultado, '[A-z]*', '', 'g'), ',', '.'), 'ã', ''), '>', ''))::decimal <= nu_referencia_unica
													then 'NORMAL'
													else 'ALTERADO'
												end
										end
								else null
							end
where 1 = 1
	and na_instituicao = 'Einstein'
	and de_analito ilike '%%covid%%'
	and de_valor_referencia is not null
;

analyze stage.stg_exam_results_full;