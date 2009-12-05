package com.goodworkalan.strata.mix;

import com.goodworkalan.go.go.Artifact;
import com.goodworkalan.mix.ProjectModule;
import com.goodworkalan.mix.builder.Builder;
import com.goodworkalan.mix.builder.JavaProject;

public class MementoProject extends ProjectModule {
    @Override
    public void build(Builder builder) {
        builder
            .cookbook(JavaProject.class)
                .produces(new Artifact("com.goodworkalan/memento/0.1"))
                .main()
                    .depends()
                        .artifact(new Artifact("com.goodworkalan/strata/0.1"))
                        .artifact(new Artifact("com.goodworkalan/fossil/0.1"))
                        .artifact(new Artifact("com.goodworkalan/pack-io/0.1"))
                        .artifact(new Artifact("com.mallardsoft/tuple-partial/0.1.0"))
                        .end()
                    .end()
                .test()
                    .depends()
                        .artifact(new Artifact("org.testng/testng/5.10/jdk15"))
                        .artifact(new Artifact("args4j/args4j/2.0.8"))
                        .artifact(new Artifact("org.mockito/mockito-core/1.6"))
                        .end()
                    .end()
                .end()
            .end();
    }
}
