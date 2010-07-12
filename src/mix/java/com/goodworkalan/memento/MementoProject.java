package com.goodworkalan.strata.mix;

import com.goodworkalan.mix.ProjectModule;
import com.goodworkalan.mix.builder.Builder;
import com.goodworkalan.mix.cookbook.JavaProject;

/**
 * Builds the project definition for Memento.
 *
 * @author Alan Gutierrez
 */
public class MementoProject implements ProjectModule {
    /**
     * Build the project definition for Memento.
     *
     * @param builder
     *          The project builder.
     */
    public void build(Builder builder) {
        builder
            .cookbook(JavaProject.class)
                .produces("com.github.bigeasy.memento/memento/0.1")
                .depends()
                    .production("com.github.bigeasy.strata/strata/0.+1")
                    .production("com.github.bigeasy.string-beans/string-beans-json/0.+1")
                    .development("org.testng/testng-jdk15/5.10")
                    .development("org.mockito/mockito-core/1.6")
                    .production("com.github.bigeasy.comfort-io/comfort-io/0.+1")
                    .end()
                .end()
            .end();
    }
}
