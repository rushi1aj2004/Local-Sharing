
simple ONU
{
    parameters:
        int onuId;
        double packetInterval @unit(s) = default(10us);
        int packetSize @unit(b) = default(1000b);
        double dataRate @unit(bps) = default(10Gbps);
        int reportSizeBytes = default(64);

        // RN-to-ONU propagation delay, sampled once
        double rnToONUDelay @unit(s) = default(uniform(5us, 25us));
    gates:
        // Normal upstream wavelength
        output out;

        // Normal downstream GATE/control path
        input grantIn;

        // Local Sharing wavelength
        output lsOut;
        input lsIn;
}
