
simple OLT
{
    parameters:
        int numONUs;
        double maxCycleTime @unit(s) = default(1ms);
        double guardTime @unit(s) = default(1us);
        int reportSizeBytes = default(64);

    gates:
        input in;
        output grantOut;
}
