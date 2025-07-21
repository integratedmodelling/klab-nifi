pipeline {
    agent {
        label 'maven-3-9-5-eclipse-temurin-21'
    }
    environment {
        TAG = "${env.BRANCH_NAME.replace('/','-')}"
        MAVEN_OPTS="-Xmx1g"
        REGISTRY = "registry.integratedmodelling.org"
        REGISTRY_CREDENTIALS = "registry-jenkins-credentials"

        VERSION_DATE = sh(
                    script: "date '+%Y-%m-%dT%H:%M:%S'",
                    returnStdout: true).trim()
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
               echo "${env.BRANCH_NAME} build with container tag: ${env.TAG}"
               withCredentials([usernamePassword(credentialsId: "${env.REGISTRY_CREDENTIALS}", passwordVariable: 'PASSWORD', usernameVariable: 'USERNAME')]) {
                   sh "./mvnw clean source:jar install -DskipTests -U "
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
                    sh 'rsync --archive --progress --delete --rsh="ssh -i ${identity} -o StrictHostKeyChecking=no" nifi-klab-nifi-api-nar/target/nifi-klab-nifi-api-nar-1.0.0-SNAPSHOT.nar bc3@192.168.250.147:/home/bc3/nifi/lib/ \
                        && nifi-klab-nifi-nar/target/nifi-klab-nifi-1.0.0-SNAPSHOT.nar'
                    sh '''
                    ssh -i ${identity} -o StrictHostKeyChecking=no bc3@192.168.250.147 '
                      export JAVA_HOME=/home/bc3/.sdkman/candidates/java/21.0.7-tem
                      export PATH=$JAVA_HOME/bin:$PATH
                      /home/bc3/nifi/bin/nifi.sh restart
                      /home/bc3/nifi/bin/nifi.sh stop
                      sleep 5
                      nohup /home/bc3/nifi/bin/nifi.sh start > /dev/null 2>&1 &
                    '
                    '''

                }
            }
        }
    }
}
