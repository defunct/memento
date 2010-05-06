package com.goodworkalan.strata.mix;

import com.goodworkalan.mix.ProjectModule;
import com.goodworkalan.mix.builder.Builder;
import com.goodworkalan.mix.builder.JavaProject;

public class MementoProject extends ProjectModule {
    @Override
    public void build(Builder builder) {
        builder
            .cookbook(JavaProject.class)
                .produces("com.github.bigeasy.memento/memento/0.1")
                .main()
                    .depends()
                        .include("com.github.bigeasy.strata/strata/0.+1")
                        .include("com.github.bigeasy.fossil/fossil/0.+1")
                        .include("com.github.bigeasy.pack/pack-io/0.+1")
                        .include("com.mallardsoft/tuple-partial/0.1.0")
                        .end()
                    .end()
                .test()
                    .depends()
                        .include("org.testng/testng-jdk15/5.10")
                        .include("args4j/args4j/2.0.8")
                        .include("org.mockito/mockito-core/1.6")
                        .end()
                    .end()
                .end()
            .end();
    }
}
