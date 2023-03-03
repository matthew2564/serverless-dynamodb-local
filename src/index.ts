import _ from "lodash";
import BbPromise from "bluebird";
import AWS from "aws-sdk";
import dynamodbLocal from "dynamodb-localhost";
import * as seeder from "./seeder";
import path from 'path';

import type Serverless from "serverless";
import type Plugin from "serverless/classes/Plugin";
import type {DynamoDBTableProps} from '@squareball/cfntypes';
import type {CreateTableInput} from "aws-sdk/clients/dynamodb";
import Service from "serverless/classes/Service";
import {Logging} from "serverless/classes/Plugin";

type Resources = Service["resources"];

type Command = Plugin.Commands[keyof Plugin.Commands];
type Options = NonNullable<Command["options"]>;

type ConvertType<Type extends string> = Type extends "string" ? string : Type extends "boolean" ? boolean : Type extends "number" ? number : never;
type ExtractOptionType<Option extends Options[keyof Options]> = Option extends { type: string, required?: boolean } ? ConvertType<Option["type"]> | (Option["required"] extends true ? unknown : undefined) : never;

type PluginCommands<P extends Plugin> = P["commands"] extends NonNullable<Plugin.Commands> ? P["commands"] : never;
type PluginSubcommands<P extends Plugin, C extends keyof PluginCommands<P> = keyof PluginCommands<P>> = PluginCommands<P>[C]["commands"];

type PluginAllCommandOptions<P extends Plugin> = {
    [C in keyof PluginCommands<P>]: {
        [SC in keyof PluginSubcommands<P>]: PluginSubcommands<P>[SC] extends { options: Options } ? {
            [SCO in keyof PluginSubcommands<P>[SC]["options"]]: ExtractOptionType<PluginSubcommands<P>[SC]["options"][SCO]>
        } : never
    }
}

type PluginCommandOptions<P extends Plugin, C extends keyof PluginCommands<P> = keyof PluginCommands<P>> = PluginAllCommandOptions<P>[C];

type PluginSubcommandOptions<P extends Plugin,
    C extends keyof PluginCommands<P> = keyof PluginCommands<P>,
    SC extends keyof PluginSubcommands<P, C> = keyof PluginSubcommands<P, C>
> = PluginCommandOptions<P, C>[SC];

type ServerlessDynamodbLocalOptions =
    PluginSubcommandOptions<ServerlessDynamodbLocal, "dynamodb", "start"> &
    PluginSubcommandOptions<ServerlessDynamodbLocal, "dynamodb", "install">;

type PluginConfig = {
    stages?: string[];
    region?: string;
    seed?: {
        [category: string]: {
            sources: {
                table: string;
                sources: string[];
                rawsources?: string[];
            }[];
        };
    }
    start: ServerlessDynamodbLocalOptions;
}

class ServerlessDynamodbLocal implements Plugin {
    config: PluginConfig;
    service: Service;
    options: Serverless.Options & ServerlessDynamodbLocalOptions;
    provider = "aws";
    hooks = {
        "dynamodb:migrate:migrateHandler": this.migrateHandler.bind(this),
        "dynamodb:seed:seedHandler": this.seedHandler.bind(this),
        "dynamodb:remove:removeHandler": this.removeHandler.bind(this),
        "dynamodb:install:installHandler": this.installHandler.bind(this),
        "dynamodb:start:startHandler": this.startHandler.bind(this),
        "before:offline:start:init": this.startHandler.bind(this),
        "before:offline:start:end": this.endHandler.bind(this),
    };
    commands = {
        dynamodb: {
            commands: {
                migrate: {
                    lifecycleEvents: ["migrateHandler"],
                    usage: "Creates local DynamoDB tables from the current Serverless configuration",
                    options: {
                        online: {
                            shortcut: "o",
                            usage: "Uses online tables (default is offline)",
                            type: "boolean"
                        },
                        convertEmptyValues: {
                            shortcut: "e",
                            usage: "Set to true if you would like the document client to convert empty values (0-length strings, binary buffers, and sets) to be converted to NULL types when persisting to DynamoDB.",
                            type: "boolean"
                        }
                    }
                },
                start: {
                    lifecycleEvents: ["startHandler"],
                    usage: "Starts local DynamoDB",
                    options: {
                        port: {
                            shortcut: "p",
                            usage: "The port number that DynamoDB will use to communicate with your application. If you do not specify this option, the default port is 8000",
                            type: "string"
                        },
                        cors: {
                            shortcut: "c",
                            usage: "Enable CORS support (cross-origin resource sharing) for JavaScript. You must provide a comma-separated \"allow\" list of specific domains. The default setting for -cors is an asterisk (*), which allows public access.",
                            type: "string"
                        },
                        inMemory: {
                            shortcut: "i",
                            usage: "DynamoDB; will run in memory, instead of using a database file. When you stop DynamoDB;, none of the data will be saved. Note that you cannot specify both -dbPath and -inMemory at once.",
                            type: "boolean"
                        },
                        dbPath: {
                            shortcut: "d",
                            usage: "The directory where DynamoDB will write its database file. If you do not specify this option, the file will be written to the current directory. Note that you cannot specify both -dbPath and -inMemory at once. For the path, current working directory is <projectroot>/node_modules/serverless-dynamodb-local/dynamob. For example to create <projectroot>/node_modules/serverless-dynamodb-local/dynamob/<mypath> you should specify -d <mypath>/ or --dbPath <mypath>/ with a forwardslash at the end.",
                            type: "string"
                        },
                        sharedDb: {
                            shortcut: "h",
                            usage: "DynamoDB will use a single database file, instead of using separate files for each credential and region. If you specify -sharedDb, all DynamoDB clients will interact with the same set of tables regardless of their region and credential configuration.",
                            type: "boolean"
                        },
                        delayTransientStatuses: {
                            shortcut: "t",
                            usage: "Causes DynamoDB to introduce delays for certain operations. DynamoDB can perform some tasks almost instantaneously, such as create/update/delete operations on tables and indexes; however, the actual DynamoDB service requires more time for these tasks. Setting this parameter helps DynamoDB simulate the behavior of the Amazon DynamoDB web service more closely. (Currently, this parameter introduces delays only for global secondary indexes that are in either CREATING or DELETING status.",
                            type: "boolean"
                        },
                        optimizeDbBeforeStartup: {
                            shortcut: "o",
                            usage: "Optimizes the underlying database tables before starting up DynamoDB on your computer. You must also specify -dbPath when you use this parameter.",
                            type: "boolean"
                        },
                        migrate: {
                            shortcut: "m",
                            usage: "After starting dynamodb local, create DynamoDB tables from the current serverless configuration.",
                            type: "boolean"
                        },
                        seed: {
                            shortcut: "s",
                            usage: "After starting and migrating dynamodb local, injects seed data into your tables. The --seed option determines which data categories to onload.",
                            type: "string"
                        },
                        heapInitial: {
                            usage: 'The initial heap size. Specify megabytes, gigabytes or terabytes using m, b, t. E.g., "2m"',
                            type: "string"
                        },
                        heapMax: {
                            usage: 'The maximum heap size. Specify megabytes, gigabytes or terabytes using m, b, t. E.g., "2m"',
                            type: "string"
                        },
                        convertEmptyValues: {
                            shortcut: "e",
                            usage: "Set to true if you would like the document client to convert empty values (0-length strings, binary buffers, and sets) to be converted to NULL types when persisting to DynamoDB.",
                            type: "boolean"
                        },
                        docker: {
                            usage: "Run DynamoDB inside docker container instead of as a local Java program",
                            type: "boolean"
                        },
                        host: {
                            usage: "The host name to use when connecting to DynamoDB. Defaults to localhost.",
                            type: "string"
                        },
                        online: {
                            shortcut: "o",
                            usage: "Uses online tables for migration and seed (default is offline)",
                            type: "boolean"
                        },
                        noStart: {
                            shortcut: "n",
                            usage: "Do not start DynamoDB local (in case it is already running)",
                            type: "boolean",
                        },
                    }
                },
                remove: {
                    lifecycleEvents: ["removeHandler"],
                    usage: "Removes local DynamoDB"
                },
                install: {
                    usage: "Installs local DynamoDB",
                    lifecycleEvents: ["installHandler"],
                    options: {
                        localPath: {
                            shortcut: "x",
                            usage: "Local dynamodb install path",
                            type: "string"
                        }
                    }

                }
            }
        }
    } as const;

    constructor(protected serverless: Serverless, options: Serverless.Options & ServerlessDynamodbLocalOptions, protected logging: Logging) {
        this.serverless = serverless;
        this.service = serverless.service;
        this.config = this.service.custom?.["dynamodb"] ?? {};
        const localPath = serverless.config && path.join(serverless.config.servicePath, '.dynamodb')
        this.options = {
            ...options,
            localPath: options.localPath ?? localPath,
        };
    }

    get port() {
        return Number.parseInt(this.config.start.port ?? "8000");
    }

    get host() {
        return this.config.start.host ?? "localhost";
    }

    /**
     * Get the stage
     *
     * @return {String} the current stage
     */
    get stage() {
        return (this.options && this.options.stage) || (this.service.provider && this.service.provider.stage);
    }

    /**
     * To check if the handler needs to be executed based on stage
     *
     * @return {Boolean} if the handler can run for the provided stage
     */
    shouldExecute() {
        return !!this.config.stages?.includes(this.stage);
    }

    dynamodbOptions(options?: { online?: boolean, convertEmptyValues?: boolean, region: string | null }) {
        let dynamoOptions = {};

        if (options?.online) {
            this.serverless.cli.log("Connecting to online tables...");
            if (!options.region) {
                throw new Error("please specify the region");
            }
            dynamoOptions = {
                region: options.region,
                convertEmptyValues: options?.convertEmptyValues ?? false,
            };
        } else {
            dynamoOptions = {
                endpoint: `http://${this.host}:${this.port}`,
                region: options?.region ?? process.env["AWS_REGION"] ?? "localhost",
                accessKeyId: process.env["AWS_ACCESS_KEY_ID"] ?? "MOCK_ACCESS_KEY_ID",
                secretAccessKey: process.env["AWS_SECRET_ACCESS_KEY"] ?? "MOCK_SECRET_ACCESS_KEY",
                convertEmptyValues: options?.convertEmptyValues ?? false,
            };
        }

        return {
            raw: new AWS.DynamoDB(dynamoOptions),
            doc: new AWS.DynamoDB.DocumentClient(dynamoOptions)
        };
    }

    migrateHandler() {
        if (this.shouldExecute()) {
            const dynamodb = this.dynamodbOptions();
            return Promise.all(this.tables.map((table) => this.createTable(dynamodb, table)));
        } else {
            this.serverless.cli.log("Skipping migration: DynamoDB Local is not available for stage: " + this.stage);
            return Promise.resolve();
        }
    }

    seedHandler() {
        if (this.shouldExecute()) {
            const options = this.options;
            const dynamodb = this.dynamodbOptions(options);

            return BbPromise.each(this.seedSources, (source) => {
                if (!source?.table) {
                    throw new Error("seeding source \"table\" property not defined");
                }
                // const seedPromise = seeder
                //     .locateSeeds(source.sources || [])
                //     .then((seeds: ItemList) =>
                //         seeder.writeSeeds(dynamodb.doc.batchWrite.bind(dynamodb.doc), source.table, seeds)
                // );
                const seedPromise = seeder
                    .locateSeeds(source.sources)
                    .then((seeds) =>
                        seeder.writeSeeds(dynamodb.doc.batchWrite.bind(dynamodb.doc), source.table, seeds))

                const rawSeedPromise = seeder
                    .locateSeeds(source?.rawsources || [])
                    .then((seeds) =>
                        seeder.writeSeeds(dynamodb.raw.batchWriteItem.bind(dynamodb.raw), source.table, seeds));

                return BbPromise.all([seedPromise, rawSeedPromise]);
            });
        } else {
            this.serverless.cli.log("Skipping seeding: DynamoDB Local is not available for stage: " + this.stage);
            return Promise.resolve();
        }
    }

    removeHandler() {
        return new BbPromise((resolve) => dynamodbLocal.remove(resolve));
    }

    installHandler() {
        const options = this.options;
        return new BbPromise((resolve) => dynamodbLocal.install(resolve, options.localPath));
    }

    startHandler() {
        if (this.shouldExecute()) {
            const options = {
                ...this.config.start,
                ...this.options,
                sharedDb: this.options.sharedDb ?? true,
                install_path: this.options.localPath
            };

            let dbPath = options.dbPath;
            if (dbPath) {
                options.dbPath = path.isAbsolute(dbPath) ? dbPath : path.join(this.serverless.config.servicePath, dbPath);
            }

            if (!this.config.start.noStart) {
                dynamodbLocal.start({
                    port: Number.parseInt(options.port ?? "8000"),
                    cors: options.cors,
                    inMemory: options.inMemory,
                    dbPath: options.dbPath,
                    docker: options.docker,
                    sharedDb: options.sharedDb,
                    delayTransientStatuses: options.delayTransientStatuses,
                    optimizeDbBeforeStartup: options.optimizeDbBeforeStartup,
                    heapInitial: options.heapInitial,
                    heapMax: options.heapMax,
                });
            }
            return BbPromise.resolve()
                .then(async () => options.migrate && await this.migrateHandler())
                .then(async () => options.seed && await this.seedHandler());
        } else {
            this.serverless.cli.log("Skipping start: DynamoDB Local is not available for stage: " + this.stage);
            return Promise.resolve();
        }
    }

    endHandler() {
        if (this.shouldExecute() && !this.config.start.noStart) {
            this.serverless.cli.log("DynamoDB - stopping local database");
            dynamodbLocal.stop(this.port);
        } else {
            this.serverless.cli.log("Skipping end: DynamoDB Local is not available for stage: " + this.stage);
        }
    }

    getDefaultStack() {
        return _.get(this.service, "resources");
    }

    getAdditionalStacks() {
        return _.values(_.get(this.service, "custom.additionalStacks", {}));
    }

    hasAdditionalStacksPlugin() {
        return this.service.plugins?.includes("serverless-plugin-additional-stacks") ?? false;
    }

    getTableDefinitionsFromStack(stack?: Resources) {
        const resources = stack?.["Resources"] ?? [];
        return Object.keys(resources)
            .filter((key) => resources[key]?.["Type"] === "AWS::DynamoDB::Table" && resources[key]?.["Properties"])
            .map(key => resources[key]?.["Properties"]);
    }

    /**
     * Gets the table definitions
     */
    get tables() {
        let stacks: Resources[] = [];

        const defaultStack = this.getDefaultStack();
        if (defaultStack) {
            stacks.push(defaultStack);
        }

        if (this.hasAdditionalStacksPlugin()) {
            stacks = stacks.concat(this.getAdditionalStacks());
        }

        return stacks.map((stack) => this.getTableDefinitionsFromStack(stack)).reduce((tables, tablesInStack) => tables.concat(tablesInStack), []);
    }

    /**
     * Gets the seeding sources
     */
    get seedSources() {
        const seedConfig = this.config.seed ?? {};
        const seed = this.options.seed || this.config.start.seed || seedConfig;
        const categories: string[] = [];
        if (typeof seed === "string") {
            categories.push(...seed.split(","));
        } else if (seed) {
            categories.push(...Object.keys(seedConfig));
        } else { // if (!seed)
            this.serverless.cli.log("DynamoDB - No seeding defined. Skipping data seeding.");
            return [];
        }
        const sourcesByCategory = categories.map((category) => seedConfig[category]?.sources);
        return _.flatten(sourcesByCategory);
    }

    async createTable(dynamodb: { raw: AWS.DynamoDB, doc: AWS.DynamoDB.DocumentClient }, cfnTable: DynamoDBTableProps) {
        if (!cfnTable.TableName || !cfnTable.KeySchema) {
            throw new Error("Table name, key schema and attribute definitions are required");
        }

        const migration: CreateTableInput = {
            TableName: cfnTable.TableName,
            KeySchema: cfnTable.KeySchema,
            AttributeDefinitions: cfnTable.AttributeDefinitions ?? [],
            LocalSecondaryIndexes: cfnTable.LocalSecondaryIndexes,
            GlobalSecondaryIndexes: cfnTable.GlobalSecondaryIndexes,
            ProvisionedThroughput: cfnTable.ProvisionedThroughput,
            Tags: cfnTable.Tags,
        }

        if (cfnTable.StreamSpecification && cfnTable.StreamSpecification.StreamViewType) {
            migration.StreamSpecification = {
                ...cfnTable.StreamSpecification,
                StreamEnabled: true
            };
        }

        if (cfnTable.SSESpecification) {
            migration.SSESpecification = {
                Enabled: cfnTable.SSESpecification.SSEEnabled
            };
        }

        if (!migration.BillingMode || migration.BillingMode === "PAY_PER_REQUEST") {
            delete migration.BillingMode;

            const defaultProvisioning = {
                ReadCapacityUnits: 5,
                WriteCapacityUnits: 5
            };
            migration.ProvisionedThroughput = defaultProvisioning;
            if (migration.GlobalSecondaryIndexes) {
                migration.GlobalSecondaryIndexes.forEach((gsi) => {
                    gsi.ProvisionedThroughput = defaultProvisioning;
                });
            }
        }

        try {
            await dynamodb.raw.createTable(migration).promise();
            this.serverless.cli.log("DynamoDB - created table " + migration.TableName);
            return migration;
        } catch (err) {
            if (err instanceof Error && err.name === 'ResourceInUseException') {
                this.serverless.cli.log(`DynamoDB - Warn - table ${migration.TableName} already exists`);
                return;
            } else {
                this.serverless.cli.log("DynamoDB - Error - ", String(err));
                throw err;
            }
        }
    }
}

export = ServerlessDynamodbLocal;
