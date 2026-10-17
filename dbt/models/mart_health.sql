select status, count(*) as attempts from {{ ref('collection_decisions') }} group by status
