/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.teradata.tpcds.query;

import com.google.common.collect.ImmutableMap;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;

/**
 * Query definition.
 *
 * <p>To generate and print all queries:</p>
 *
 * <blockquote>
 *     final Generator generator = Query.generator(new Random(0));
 *     for (Query q : Query.values()) {
 *         System.out.println(q.generate(generator));
 *     }
 * </blockquote>
 */
public enum Query {
    Q01, Q02, Q03, Q04, Q05, Q06, Q07, Q08, Q09,
    Q10, Q11, Q12, Q13, Q14, Q15, Q16, Q17, Q18, Q19,
    Q20, Q21, Q22, Q23, Q24, Q25, Q26, Q27, Q28, Q29,
    Q30, Q31, Q32, Q33, Q34, Q35, Q36, Q37, Q38, Q39,
    Q40, Q41, Q42, Q43, Q44, Q45, Q46, Q47, Q48, Q49,
    Q50, Q51, Q52, Q53, Q54, Q55, Q56, Q57, Q58, Q59,
    Q60, Q61, Q62, Q63, Q64, Q65, Q66, Q67, Q68, Q69,
    Q70, Q71, Q72, Q73, Q74, Q75, Q76, Q77, Q78, Q79,
    Q80, Q81, Q82, Q83, Q84, Q85, Q86, Q87, Q88, Q89,
    Q90, Q91, Q92, Q93, Q94, Q95, Q96, Q97, Q98, Q99;

    public final int id;
    public final String template;
    public final ImmutableMap<String, Substitution> args;

    private static final Substitution EMPTY = Substitutions.fixed("");

    private static final ImmutableMap<String, Substitution> BUILTIN_ARGS =
        ImmutableMap.<String, Substitution>builder()
            .put("__LIMITA", EMPTY)
            .put("__LIMITB", EMPTY)
            .put("__LIMITC", Substitutions.fixed("LIMIT %d"))
            .put("_QUERY", EMPTY)
            .put("_STREAM", EMPTY)
            .put("_TEMPLATE", EMPTY)
            .put("_BEGIN", EMPTY)
            .put("_END", EMPTY)
            .build();

    Query()
    {
        id = Integer.valueOf(name().substring(1));
        Init init;
        try {
            init = new Init();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        template = init.template;
        args = ImmutableMap.copyOf(init.args);
    }

    /** Returns the query with a given id. (1 &le; {@code id} &le; 99.)
     *
     * @param id Query id
     * @return Query with given id
     */
    public static Query of(int id)
    {
        return values()[id - 1];
    }

    /** Returns the substitutions in this query. Each has a name and a
     * generator. */
    ImmutableMap<String, Substitution> substitutions()
    {
        final ImmutableMap.Builder<String, Substitution> result =
            ImmutableMap.builder();
        final Substitution limitArg = args.get("_LIMIT");
        if (limitArg != null) {
            final String limitString = Substitutions.constant(limitArg);
            final int limit = Integer.parseInt(limitString);
            final Function<String, String> transform =
                input -> String.format(input, limit);
            result.put("_LIMITA",
                Substitutions.transform(BUILTIN_ARGS.get("__LIMITA"), transform));
            result.put("_LIMITB",
                Substitutions.transform(BUILTIN_ARGS.get("__LIMITB"), transform));
            result.put("_LIMITC",
                Substitutions.transform(BUILTIN_ARGS.get("__LIMITC"), transform));
        }
        for (Map.Entry<String, Substitution> arg : args.entrySet()) {
            result.put(arg.getKey(), arg.getValue());
            if (arg.getValue() instanceof Substitutions.ListSubstitution) {
                final Substitutions.ListSubstitution listSubstitution =
                    (Substitutions.ListSubstitution) arg.getValue();
                for (int i = 0; i < listSubstitution.count; i++) {
                    result.put(arg.getKey() + "." + (i + 1),
                        Substitutions.item(arg.getKey(), i));
                }
            }
        }
        return result.build();
    }

    /** Returns the SQL query, by expanding all embedded variables using the
     * given random-number generator.
     *
     * @param random Random-number generator
     *
     * @return Query string
     */
    public String sql(Random random)
    {
        String s = template;
        final Generator generator = new Generator(random, substitutions());

        for (Map.Entry<String, Substitution> entry
            : generator.substitutions.entrySet()) {
            final String key = entry.getKey();
            final String seek = "[" + key + "]";
            if (s.contains(seek)) {
                final Substitution substitution = entry.getValue();
                String value = substitution.generate(generator);
                s = s.replace(seek, value);
            }
        }
        return s;
    }

    /** Contains state for initializing a query. */
    private class Init
    {
        String template;
        final Map<String, Substitution> args = new LinkedHashMap<>();

        Init() throws IOException
        {
            final InputStream stream =
                Query.class.getResourceAsStream("/query_templates/query" + id
                                                + ".tpl");
            final BufferedReader reader =
                new BufferedReader(new InputStreamReader(stream));
            final StringBuilder buf = new StringBuilder();
            while (true) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                if (line.startsWith("--")) {
                    continue;
                }
                if (line.matches("^ *$")) {
                    continue;
                }
                if (line.matches("^ *[Dd]efine .*$")) {
                    line = line.trim();
                    int eq = line.indexOf('=');
                    assert eq >= 0;
                    String name = line.substring("define ".length(), eq).trim();
                    String rest = line.substring(eq + 1, line.length() - 1);
                    rest = rest.replaceAll("--.*", "");
                    rest = rest.replaceAll("; *$", "");
                    rest = rest.replaceAll("^ *", "");
                    args.put(name.toUpperCase(Locale.ROOT), Substitutions.parse(rest));
                }
                else {
                    buf.append(line).append("\n");
                }
            }
            template = buf.toString().replaceAll(" *; *$", "");
        }
    }
}

// End Query.java
