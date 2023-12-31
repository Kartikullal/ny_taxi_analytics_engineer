 {#
    This macro returns the description of the rate id 
#}

{% macro get_rate_id_type_description(ratecodeid) -%}

    case {{ ratecodeid }}
        when 1 then 'Standard rate'
        when 2 then 'JFK'
        when 3 then 'Newark'
        when 4 then 'Nassau or Westchester'
        when 5 then 'Negotiated fare'
        when 6 then 'Group ride'
    end

{%- endmacro %}

