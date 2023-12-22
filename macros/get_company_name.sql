 {#
    This macro returns the name of the company based on license 
#}

{% macro get_company_name(hvfhs_license_num) -%}

    case {{ hvfhs_license_num }}
        when 'HV0002' then 'Juno'
        when 'HV0003' then 'Uber'
        when 'HV0004' then 'Via'
        when 'HV0005' then 'Lyft'
    end

{%- endmacro %}

              