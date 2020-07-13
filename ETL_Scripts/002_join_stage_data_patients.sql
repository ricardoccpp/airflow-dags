drop table if exists stage.stg_patients_full;

select *
into stage.stg_patients_full
from (
	select
		id_paciente,
		case when trim(ic_sexo) = ''
			then null
			else ic_sexo
		end as ic_sexo,
		case when regexp_match(aa_nascimento, '[A-z]') is not null or trim(aa_nascimento) = ''
			then null
			else aa_nascimento
		end::smallint as aa_nascimento,
		case when trim(cd_pais) = ''
			then null
			else cd_pais
		end as cd_pais,
		case when trim(cd_uf) = ''
			then null
			else cd_uf
		end as cd_uf,
		case when cd_municipio = 'MMMM' or trim(cd_municipio) = ''
			then null
			else cd_municipio
		end as cd_municipio,
		case when regexp_match(cd_cep, '[A-z]') is not null or trim(cd_cep) = ''
			then null
			else cd_cep
		end as cd_cep,
		'Einstein' as na_instituicao,
		now() as dt_load
	from stage.stg_patients_einstein
	union
	select
		id_paciente,
		case when trim(ic_sexo) = ''
			then null
			else ic_sexo
		end as ic_sexo,
		case when regexp_match(aa_nascimento, '[A-z]') is not null or trim(aa_nascimento) = ''
			then null
			else aa_nascimento
		end::smallint as aa_nascimento,
		case when trim(cd_pais) = ''
			then null
			else case trim(cd_pais)
				when 'Brasil'
					then 'BR'
				when 'Alemanha'
					then 'DE'
				when 'AfeganistĂŁo'
					then 'AF'
				when 'Filipinas'
					then 'PH'
				else trim(cd_pais)
			end
		end as cd_pais,
		case when trim(cd_uf) = ''
			then null
			else cd_uf
		end as cd_uf,
		case when cd_municipio = 'MMMM' or trim(cd_municipio) = ''
			then null
			else cd_municipio
		end as cd_municipio,
		case when regexp_match(cd_cep, '[A-z]') is not null or trim(cd_cep) = ''
			then null
			else cd_cep
		end as cd_cep,
		'Fleury' as na_instituicao,
		now() as dt_load
	from stage.stg_patients_fleury
	union
	select
		id_paciente,
		case when trim(ic_sexo) = ''
			then null
			else ic_sexo
		end as ic_sexo,
		case when regexp_match(aa_nascimento, '[A-z]') is not null or trim(aa_nascimento) = ''
			then null
			else aa_nascimento
		end::smallint as aa_nascimento,
		case when trim(cd_pais) = ''
			then null
			else case trim(cd_pais)
				when 'Brasil'
					then 'BR'
				when 'Alemanha'
					then 'DE'
				when 'AfeganistĂŁo'
					then 'AF'
				when 'Filipinas'
					then 'PH'
				else trim(cd_pais)
			end
		end as cd_pais,
		case when trim(cd_uf) = ''
			then null
			else cd_uf
		end as cd_uf,
		case when cd_municipio = 'MMMM' or trim(cd_municipio) = ''
			then null
			else cd_municipio
		end as cd_municipio,
		case when regexp_match(cd_cep, '[A-z]') is not null or trim(cd_cep) = ''
			then null
			else cd_cep
		end as cd_cep,
		'Sírio Libanês' as na_instituicao,
		now() as dt_load
	from stage.stg_patients_sirio
) x
;