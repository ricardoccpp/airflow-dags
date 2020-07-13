drop table if exists stage.stg_exam_results_full;

select *
into stage.stg_exam_results_full
from (
	select
		id_paciente,
		'Not applied' as id_atendimento,
		to_date(dt_coleta, 'dd/mm/yyyy') as dt_coleta,
		trim(de_origem) as de_origem,
		trim(de_exame) as de_exame,
		trim(de_analito) as de_analito,
		trim(de_resultado) as de_resultado,
		trim(cd_unidade) as cd_unidade,
		trim(de_valor_referencia) as de_valor_referencia,
		'Einstein' as na_instituicao,
		now() as dt_load
	from stage.stg_exam_results_einstein
	union
	select
		id_paciente,
		'Not applied' as id_atendimento,
		to_date(dt_coleta, 'dd/mm/yyyy') as dt_coleta,
		trim(de_origem) as de_origem,
		trim(de_exame) as de_exame,
		trim(de_analito) as de_analito,
		trim(de_resultado) as de_resultado,
		trim(cd_unidade) as cd_unidade,
		trim(de_valor_referencia) as de_valor_referencia,
		'Fleury' as na_instituicao,
		now() as dt_load
	from stage.stg_exam_results_fleury
	union
	select
		id_paciente,
		id_atendimento,
		to_date(dt_coleta, 'yyyy-mm-dd') as dt_coleta,
		trim(de_origem) as de_origem,
		trim(de_exame) as de_exame,
		trim(de_analito) as de_analito,
		trim(de_resultado) as de_resultado,
		trim(cd_unidade) as cd_unidade,
		trim(de_valor_referencia) as de_valor_referencia,
		'Sírio Libanês' as na_instituicao,
		now() as dt_load
	from stage.stg_exam_results_sirio
) x
;