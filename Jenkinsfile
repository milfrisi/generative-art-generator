#!/usr/bin/env groovy
args = [
  branchMap: [
    "main": [
      keytab: "mpmprod"
    ],
    "dev": [
      keytab: "mpmdev"
    ],
    "safe-deploy": [
      keytab: "mpmdev"
    ]
  ],
  testingKeytab: "mpmdev",
]

KEYTAB = null
DEPLOY = false
SLACK_CHANNEL = args.get('slackChannel')

if(args.branchMap == null) {
  throw new Exception("ERROR: no branch map provided!")
}

pipeline {

  agent {
    /*
    In the current configuration, the jenkins-agent-k8s agent specified on top of the file is a pod (1 pod per pipeline) that contains 3 containers:
    - jnlp : Jenkins JNLP-Agent for the connection to Jenkins master and some steps like the checkout and git stuff
    - docker-compose : container with docker and docker-compose binaries (using the hosts docker sock) for all the docker stuff
    - kubectl-helm : for interaction with the cluster
    Inside every step, you can choose which container to use for what.
    */
    label "jenkins-agent-k8s"
  }

  options {
    skipStagesAfterUnstable()
  }

  parameters {
    booleanParam(
      name: "DEPLOY_ENV",
      description: "Check to enable the environment deployment.",
      defaultValue: false,
    )
  }

  stages {

    stage('init') {
      steps {
        script {

          // set KEYTAB and DEPLOY vars
          try {
            KEYTAB = args.branchMap[env.BRANCH_NAME].keytab
            DEPLOY = true
          } catch (NullPointerException e) {
            KEYTAB = args.testingKeytab
            DEPLOY = false
            println("INFO: Branch ${env.BRANCH_NAME} not present in branchMap.")
            println("INFO: Using default keytab (args.testingKeytab).")
          }
          // check that KEYTAB is valid
          if (KEYTAB == null) {
            error "ERROR: Also no keytab or default keytab specified... Exiting."
          }
          // check that we're not just indexing the branch
          if (currentBuild.rawBuild.getCauses().toString().contains('BranchIndexingCause')) {
            print "INFO: Branch Indexing (we'll skip the deployment step)."
            DEPLOY = false
          }

          container('docker') {
            withCredentials([file(credentialsId: "keytab-${KEYTAB}", variable: "KEYTAB_FILE")]) {
              sh """
                ln -sfv conf/deploy/docker-compose.jenkins.yml docker-compose.override.yml
                cp $KEYTAB_FILE conf/krb5.keytab
                docker-compose config
                docker-compose up -d
                docker-compose exec -T cmd ./conf/deploy/apt.sh
                
              """
              env.RUNNING_JOBS=sh (returnStdout: true, script:"""
                # check if pipeline is running
                docker-compose exec -T cmd ./conf/deploy/runningjobs.sh
              """
              ).trim()
            }
          }

          if (RUNNING_JOBS != '0') {
            print("ERROR: The workflow from this project is still running! Canceling deployment...")
            DEPLOY = false
          } else {
            print("INFO: There is no workflow running. Continue to deployment...")
          }

          // if DEPLOY=true & RUUNING_JOBS>0 then DEPLOY=false
          // println("Info: Setting deploy to false")

          println("BRANCH_NAME: ${env.BRANCH_NAME}")
          println("DEPLOY_ENV: ${params.DEPLOY_ENV}")
          println("DEPLOY: ${DEPLOY}")
          println("KEYTAB: ${KEYTAB}")
          println("SLACK_CHANNEL: ${SLACK_CHANNEL}")            
          println("RUNNING JOBS: ${RUNNING_JOBS}")          
        }
      }
    }

    stage('validate') {
      steps {
        script {
          container('docker') {
            sh "docker-compose exec -T cmd validate"
          }
        }
      }
    }

    stage('deploy-env') {
      when { equals expected: true, actual: (params.DEPLOY_ENV && DEPLOY) }
      steps {
        script {
          container('docker') {
            sh "docker-compose exec -T cmd deploy-env"
          }
        }
      }
    }

    stage('deploy-src') {
      when { equals expected: true, actual: DEPLOY }
      steps {
        script {
          container('docker') {
            sh "docker-compose exec -T cmd deploy-src"
          }
        }
      }
    }
  }

  post {

    success {
      script {
        if (DEPLOY && SLACK_CHANNEL) {
          slackNotify("Project successfully deployed", "${SLACK_CHANNEL}")
        }
      }
    }

    unstable {
      script {
        if (SLACK_CHANNEL) {
          slackAlert("Project marked as unstable!", "${SLACK_CHANNEL}")
        }
      }
    }

    failure {
      script {
        if (SLACK_CHANNEL) {
          slackAlert("Project failed to deploy!", "${SLACK_CHANNEL}")
        }
      }
    }

    cleanup {
      script {
        container('docker') {
          // clean up docker-compose leftovers (especially the network)
          sh "docker-compose down -v"
        }
      }
    }
  }
}