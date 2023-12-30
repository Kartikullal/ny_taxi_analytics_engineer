 {#
    This macro returns the name of the vendor based on license 
#}

{% macro get_vendor_name(vendorid) -%}

    case {{ vendorid }}
        when 1 then 'Creative Mobile Technologies, LLC'
        when 2 then 'VeriFone Inc.'
    end

{%- endmacro %}

              