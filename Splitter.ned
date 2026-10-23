
simple Splitter
{
    parameters:
        int numONUs;

    gates:
        // Normal upstream: ONU -> Splitter -> OLT
        input in[numONUs];
        output out;

        // Normal downstream: OLT -> Splitter -> ONU
        input grantIn;
        output grantOut[numONUs];

        // Local Sharing: ONU -> Splitter -> all ONUs
        input lsIn[numONUs];
        output lsOut[numONUs];
}
