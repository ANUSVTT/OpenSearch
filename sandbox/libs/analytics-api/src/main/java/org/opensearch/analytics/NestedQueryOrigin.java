/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics;

/**
 * [NESTED] Per-query marker: did the ORIGINAL query text use the explicit {@code expand} command?
 *
 * <p>Semantics differ by syntax: a dotted nested reference ({@code where subs.views > 40}) is a
 * PARENT-level existence check (vanilla nested semantics — the parent row set never multiplies),
 * while explicit {@code expand} ({@code expand subs | where views > 40}) is a user-requested
 * per-child flatten (Splunk mvexpand semantics). By the time plans reach the Substrait post-pass
 * the two shapes are not reliably distinguishable (isthmus/pushdown can collapse the projections
 * that differ), so the parent-dedup pass reads this marker to decide whether to restore parent
 * row arity.
 *
 * <p>Set by {@code DefaultPlanExecutor} from {@code QueryRequestContext#querySource()} (the
 * original user text) around fragment conversion; read by {@code NestedParentDedupRewriter}.
 * Same-thread contract: conversion runs synchronously on the executor thread that sets the flag,
 * and the {@code finally} clear prevents leakage across pooled-thread reuse.
 *
 * @opensearch.internal
 */
public final class NestedQueryOrigin {

    private static final ThreadLocal<Boolean> EXPLICIT_EXPAND = new ThreadLocal<>();

    private NestedQueryOrigin() {}

    /** Marks the current thread's in-flight query as explicit-expand ({@code true}) or dotted origin. */
    public static void setExplicitExpand(boolean explicitExpand) {
        EXPLICIT_EXPAND.set(explicitExpand);
    }

    /** True only when the original query text used the {@code expand} command explicitly. */
    public static boolean isExplicitExpand() {
        return Boolean.TRUE.equals(EXPLICIT_EXPAND.get());
    }

    /** Clears the marker; call in a {@code finally} after fragment conversion. */
    public static void clear() {
        EXPLICIT_EXPAND.remove();
    }

    /**
     * True if the given PPL text contains an explicit {@code expand} command (a pipe followed by
     * the {@code expand} keyword). Callers pass the ORIGINAL user text from
     * {@code QueryRequestContext#querySource()} — never a rewritten/translated form.
     */
    public static boolean textUsesExplicitExpand(String pplText) {
        return pplText != null && pplText.matches("(?is).*\\|\\s*expand\\b.*");
    }
}
