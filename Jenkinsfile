properties([
  pipelineTriggers([
    githubPush()
  ])
])

pipeline {
    agent {
        label 'maven-3-9-5-eclipse-temurin-21'
    }
    environment {
        TAG = "${env.BRANCH_NAME.replace('/','-')}"
        MAVEN_OPTS="-Xmx1g"
        //MINIO_HOST = "http://192.168.250.150:9000"
        //MINIO_CREDENTIALS = "jenkins-ci-minio"
        REGISTRY = "registry.integratedmodelling.org"
        REGISTRY_CREDENTIALS = "registry-jenkins-credentials"

        VERSION_DATE = sh(
                    script: "date '+%Y-%m-%dT%H:%M:%S'",
                    returnStdout: true).trim()
        /*
        RESOURCES_CONTAINER = "resources-service-21"
        RESOURCE_SERVICE = "resources"
        RUNTIME_CONTAINER = "runtime-service-21"
        RUNTIME_SERVICE = "runtime"
        RESOLVER_CONTAINER = "resolver-service-21"
        RESOLVER_SERVICE = "resolver"
        REASONER_CONTAINER = "reasoner-service-21"
        REASONER_SERVICE = "reasoner"
        BASE_CONTAINER = "klab-base-21:dd2b778c852f20ad9c82fe6e12d5723e23e3dd19"
        */
        DOCKER_HOST = "192.168.250.215"
        DOCKER_STACK = "klab"
    }
    stages {
        stage('Build') {
            steps {
                script {
                    currentBuild.description = "${env.BRANCH_NAME} build with container tag: ${env.TAG}"
                }
                sh './mvnw clean source:jar package -DskipTests'
            }
        }
        stage('Install') {
            steps {
               script {
                   //jibBuild = 'jib:build -Djib.httpTimeout=180000'
                   //dockerBuild = sh(script: "git log -1 --pretty=%B | grep -qi '\\[docker build\\]'", returnStatus: true)
                   //env.JIB = (env.BRANCH_NAME == 'master' || env.BRANCH_NAME == 'develop' || dockerBuild == 0) ? jibBuild : ''
                   env.JIB = ''
               }
               echo "${env.BRANCH_NAME} build with container tag: ${env.TAG}"
               withCredentials([usernamePassword(credentialsId: "${env.REGISTRY_CREDENTIALS}", passwordVariable: 'PASSWORD', usernameVariable: 'USERNAME')]) {
                   sh "./mvnw clean source:jar install -DskipTests -U ${env.JIB}"
               }
            }
        }
        stage('Deploy artifacts') {
            /*when {
                anyOf { branch 'develop'; branch 'master' }
            }*/
            steps {
                withCredentials([sshUserPrivateKey(credentialsId: 'jenkins-im-communication', keyFileVariable: 'identity')]) {
                    sh './mvnw --projects nifi-klab-nifi-api-nar javadoc:javadoc'
                    sh 'rsync --archive --progress --delete --rsh="ssh -i ${identity} -o StrictHostKeyChecking=no" nifi-klab-nifi-api-nar/target/nifi-klab-nifi-api-nar-1.0.0-SNAPSHOT.nar bc3@192.168.250.147:repo/im-nifi/nar/'
                }
            }
        }
    }
}
