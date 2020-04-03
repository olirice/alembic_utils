CREATE OR REPLACE FUNCTION public.to_upper(some_text text)
RETURNS TEXT AS
$$
    SELECT upper(some_text)
$$ language SQL;
