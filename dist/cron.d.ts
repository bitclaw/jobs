export type ParsedCron = {
    minutes: Set<number>;
    hours: Set<number>;
    daysOfMonth: Set<number>;
    months: Set<number>;
    daysOfWeek: Set<number>;
};
export declare function parseCron(expression: string): ParsedCron;
export declare function cronMatches(parsed: ParsedCron, date: Date): boolean;
export declare function nextCronOccurrence(parsed: ParsedCron, after: Date): Date;
//# sourceMappingURL=cron.d.ts.map